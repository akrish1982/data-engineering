import re
import ast
import time
import json
import inspect
from threading import Thread
from pyspark.sql import SparkSession
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession

from pyspark.sql.types import Row
from pyspark.sql.functions import col, regexp, current_user, asc, lower, nvl, lit

spark = None
warehouse_id = None
w = WorkspaceClient()
cluster_id = None


class ThreadWithReturnValue(Thread):
    def __init__(self, group=None, target=None, name=None, args=[], kwargs={}, Verbose=None):
        Thread.__init__(self, group, target, name, args, kwargs)
        self._return = None

    def run(self):
        if self._target is not None:
            try:
                self._return = self._target(*self._args, **self._kwargs)
            except Exception as ex:
                self._return = Exception(f'Exception in {self._target.__name__}: {ex}')

    def join(self, *args):
        Thread.join(self, *args)
        return self._return


class ThreadList(list):
    def append(self, thread) -> None:
        if not isinstance(thread, ThreadWithReturnValue):
            raise TypeError(f'thread must be of an instance of ThreadWithReturnValue not {type(thread)}')
        return super().append(thread)
    
    def start_all_threads(self) -> None:
        for thread in self:
            thread.start()

    def join_all_threads(self) -> dict:
        final_res = {}
        exception_lst = []
        for thread in self:
            res = thread.join()
            if res and isinstance(res, dict):
                final_res.update(res)
            elif res and isinstance(res, Exception):
                exception_lst.append(res)
        
        if exception_lst:
            for i, exception in enumerate(exception_lst, start=1):
                print(f'---- EXCEPTION #{i} START ----\n\n{str(exception).strip()}\n\n---- EXCEPTION #{i} END ----')
                if i < len(exception_lst):
                    print('\n\n')
            print()
            raise Exception('GOT THE ABOVE EXCEPTIONS PLEASE REVIEW')
        
        return final_res

    def start_and_join_all_threads(self):
        self.start_all_threads()
        return self.join_all_threads()


class Table():
    def __init__(self, full_tbl_name, full_copy_name: str=None, exclude_cols: list=None, col_mapping: dict=None, column_exprs: dict=None, data_type_exprs: dict=None) -> None:
        self.full_tbl_name = full_tbl_name
        self.catalog_name, self.schema_name, self.tbl_name = full_tbl_name.split('.')
        
        if full_copy_name is not None:
            self.full_copy_name = full_copy_name.replace('%CATALOG_NAME%', self.catalog_name).replace('%SCHEMA_NAME%', self.schema_name).replace('%TBL_NAME%', self.tbl_name)
            self.copy_catalog_name, self.copy_schema_name, self.copy_tbl_name = self.full_copy_name.split('.')
            assert self.full_copy_name != self.full_tbl_name, f'TABLE NAME: {self.full_tbl_name} CANNOT EQUAL COPY NAME: {self.full_copy_name}'
        else:
            self.full_copy_name, self.copy_catalog_name, self.copy_schema_name, self.copy_tbl_name = None, None, None, None

        self.exclude_cols = []
        if exclude_cols is not None:
            self.exclude_cols = [col.lower() for col in exclude_cols]

        self.col_mapping = {}
        if col_mapping is not None:
            self.col_mapping = {from_col.lower(): to_col.lower() for from_col, to_col in col_mapping.items()}

        self.column_exprs = {}
        if column_exprs is not None:
            self.column_exprs = {dt: expr for dt, expr in column_exprs.items()}

        self.data_type_exprs = {}
        if data_type_exprs is not None:
            self.data_type_exprs = {dt: expr for dt, expr in data_type_exprs.items()}

        self.col_info = self.get_tbl_col_info()


    def get_tbl_col_info(self) -> dict:
        col_info = []
        for idx, row in enumerate(execute_sql_query(f'DESC {self.full_tbl_name}', as_df=True).collect()):
            if row['col_name'] == '# Partition Information':
                break
            if row['col_name'].lower() in self.exclude_cols:
                continue
            
            tmp = {'name': row['col_name'].lower(), 'data_type': row['data_type'].lower(), 'idx': idx}
            if tmp['name'] in self.col_mapping:
                tmp['alias'] = self.col_mapping[tmp['name']]

            # If the column has a expr
            if self.column_exprs.get(tmp['name'], '') != '':
                tmp['col_expr'] = self.column_exprs[tmp['name']]

            # if re.search(r'\s+', tmp.get('alias', tmp['name'])):
            #     tmp['name'] = f"`{tmp['name']}`"
            #     tmp['alias'] = re.sub(r'\s+', '_', tmp.get('alias', tmp['name']))

            # If the columns datatype has an expr
            if self.data_type_exprs.get(tmp['data_type'], '') != '':
                tmp['dt_expr'] = self.data_type_exprs[tmp['data_type']]

            col_info.append(tmp)
        return col_info
    

    def get_col_lst(self, aliased: bool=False) -> list:
        return [col.get('alias', col['name']) if aliased else col['name'] for col in self.col_info]


    # def get_col_csv(self, aliased: bool=False, use_dt_expr: bool=False):
    #     ret = []
    #     for col in self.col_info:
    #         col_name = col['name']
    #         col_dt = col['data_type']

    #         # If the columns datatype has an expr
    #         dt_expr = self.data_type_exprs[col_dt.lower()].format(__COLUMN_NAME__=col_name) if use_dt_expr and col_dt in self.data_type_exprs else ''

    #         str_ret = dt_expr if dt_expr else col_name

    #         # If the column has an alias add that if the alised flag is true
    #         # If there is a dt_expr and the alias is false we only want the column name
    #         str_ret += f" AS {col.get('alias', col_name) if aliased else col_name}" if aliased or dt_expr else ''
            
    #         ret.append(str_ret)
            
    #     return ','.join(ret)

    def get_col_expr_str(self, col, aliased: bool=False, use_col_expr: bool=False, use_dt_expr: bool=False):
        col_name = col['name']

        ret = f'{col_name}'
        if use_col_expr and 'col_expr' in col:
            ret = col['col_expr'].format(__COLUMN_NAME__=ret)

        if use_dt_expr and 'dt_expr' in col:
            ret = col['dt_expr'].format(__COLUMN_NAME__=ret)
        
        if aliased and 'alias' in col:
            ret += f" AS {col['alias']}"
        elif (use_col_expr and 'col_expr' in col) or (use_dt_expr and 'dt_expr' in col):
            ret += f" AS {col_name}"

        return ret


    def get_col_csv(self, aliased: bool=False, use_col_expr: bool=False, use_dt_expr: bool=False):
        return ','.join([self.get_col_expr_str(col, aliased=aliased, use_col_expr=use_col_expr, use_dt_expr=use_dt_expr) for col in self.col_info])
    

    def get_copy_ddl(self, copy_type: str, filter_clause: str=None, aliased: bool=False) -> str:
        copy_type = copy_type.upper().strip()
        assert copy_type in ('TABLE', 'VIEW'), f'INVALID COPY TYPE: {copy_type}, ONLY TABLE AND VIEW ARE SUPPORTED'
        ddl = f'CREATE OR REPLACE {copy_type} {self.full_copy_name} AS SELECT {self.get_col_csv(aliased=aliased, use_col_expr=True, use_dt_expr=True)} FROM {self.full_tbl_name}'
        if filter_clause:
            ddl += filter_clause
        return ddl
    

    def set_exclude_cols(self, exclude_cols: list) -> None:
        new_exclude_cols = [col.lower() for col in exclude_cols]
        if len(list(set(self.exclude_cols) - set(new_exclude_cols))):
            self.exclude_cols = new_exclude_cols
            self.col_info = self.get_tbl_col_info()


def validate_function_args(func, args:dict) -> None:
    func_params = inspect.signature(func).parameters
    for arg_name in func_params:
        arg = func_params[arg_name]
        arg_hint_type, arg_default = arg.annotation, arg.default
        
        # This must mean it is a required arg since it does not have a default
        # if arg.default == arg.empty:
        #     print(arg)

        # Make sure the type matches, unless it equals the default value
        if not isinstance(args[arg_name], arg_hint_type) and arg_default != args[arg_name]:
            raise TypeError(f'Invalid type for arg {arg_name}, expected {arg_hint_type}, got {type(args[arg_name])}')


def execute_sql_query(query: str, as_df: bool=False, warehouse_id=None):
    if warehouse_id is None:
        if 'warehouse_id' not in globals() or globals()['warehouse_id'] is None:
            raise Exception('WHEN IMPORTING execute_sql_query warehouse_id MUST BE PASSED AS A PARAM')
        else:
            warehouse_id = globals()['warehouse_id']

    res = w.statement_execution.execute_statement(query, warehouse_id, wait_timeout='0s')
    
    wait_states = ['PENDING', 'RUNNING']

    try:
        time.sleep(1)
        res = w.statement_execution.get_statement(res.statement_id)

        # Wait for query to complete and cancel it if a keyboard interrupt is detected
        while res.status.state.value in wait_states:
            res = w.statement_execution.get_statement(res.statement_id)
            time.sleep(5)

    except KeyboardInterrupt:
        w.statement_execution.cancel_execution(res.statement_id)
        raise
    
    if res.status.state.value == 'FAILED':
        raise Exception(res.status.error.message)
    elif res.status.state.value == 'CANCELED':
        raise Exception(f'QUERY HAS BEEN CANCELED, ID: {res.statement_id}')
    elif res.status.state.value not in wait_states and res.status.state.value != 'SUCCEEDED':
        raise Exception(f'UNKNOWN STATE {res.status.state.value} FOR QUERY ID: {res.statement_id}')

    if as_df:
        res_dict = res.as_dict()
        col_lst = sorted(res_dict['manifest']['schema']['columns'], key=lambda x: x['position'])

        # df = pd.DataFrame(
        #     res_dict.get('result', {}).get('data_array', [])
        #     , columns=[col['name'] for col in col_lst]
        # )

        # dt_map = {
        #     'INT': int
        #     , 'BOOLEAN': bool
        #     , 'STRING': str
        #     , 'DATE': 'datetime64'
        #     , 'TIMESTAMP': 'datetime64'
        # }
        # df = df.astype({col['name']: dt_map[col['type_name']] for col in col_lst}).convert_dtypes()

        # Using spark
        df = spark.createDataFrame(res_dict.get('result', {}).get('data_array', []), schema=','.join([f"`{col['name']}` STRING" for col in col_lst]))
        for col in col_lst:
            df = df.withColumn(col['name'], df[f"`{col['name']}`"].cast(col['type_name']))

        return df
        
    return res


def get_save_to_cfg_tbl_args():
    # Args that are columns in the cfg tbl
    col_name_lst = [
        'db_tbl'
        , 'sf_tbl'
        , 'user_id'
    ]
    
    # Args that are rows in the cfg tbl
    arg_name_lst = [
        'primary_keys'
        , 'filter_clause'
        , 'exclude_cols'
        , 'db_exclude_cols'
        , 'sf_exclude_cols'
        , 'column_mapping'
        , 'warehouse_id'
        , 'cfg_json'
        , 'column_exprs'
        , 'data_type_exprs'
    ]

    return {arg_name: 'ROW' if arg_name in arg_name_lst else 'COL' for arg_name in (arg_name_lst + col_name_lst)}


def get_default_arg_map() -> dict:
    return {
        'db_tbl': {'required': True, 'type': str}
        , 'sf_tbl': {'required': True, 'type': str}
        , 'filter_clause': {'required': False, 'type': str}
        , 'warehouse_id': {'required': False, 'type': str, 'default': '0e9c378a506f69a9'}
        # Will set the default for this later, dont set it here
        , 'missmatch_tbl_name': {'required': False, 'type': str}
        , 'cfg_tbl_name': {'required': False, 'type': str, 'default': 'users.pamons.validation_config'}
        , 'sf_copy_obj_type': {'required': False, 'type': str, 'default': 'TABLE'}
        , 'db_copy_obj_type': {'required': False, 'type': str, 'default': 'VIEW'}
        , 'json_ordering_udf': {'required': False, 'type': str, 'default': 'users.pamons.sort_json_str_keys'}
        , 'exclude_cols': {'required': False, 'type': list}
        , 'db_exclude_cols': {'required': False, 'type': list}
        , 'sf_exclude_cols': {'required': False, 'type': list}
        , 'primary_keys': {'required': False, 'type': list}
        , 'skip_checks': {'required': False, 'type': list}
        # List of dicts
        , 'column_mapping': {
            'required': False, 'type': list
            , 'arg_map': {
                'sf': {'required': True, 'type': str} 
                , 'db': {'required': True, 'type': str} 
            }
        }
        , 'column_exprs': {
            'required': False, 'type': dict, 'lower_case_keys': True
            # Since the keys can differ because they are column names we cannot make an arg map
            , 'skip_arg_map_check': True
        }
        , 'data_type_exprs': {
            'required': False, 'type': dict
                , 'arg_map': {
                    'timestamp': {'required': False, 'type': str} 
                    , 'date': {'required': False, 'type': str}
                    , 'double': {'required': False, 'type': str}
                }
                , 'lower_case_keys': True
                , 'default': {'timestamp': 'date_trunc("SECOND", {__COLUMN_NAME__})'}
        }
        , 'fail_at_first_check': {'required': False, 'type': bool}
        , 'include_queries': {'required': False, 'type': bool}
        , 'use_threads': {'required': False, 'type': bool}
        , 'use_cfg_tbl': {'required': False, 'type': bool}
        # 0 meaning no table will be created
        , 'num_of_sample_rows': {'required': False, 'type': int, 'default': 0}

        # Debug only flags
        , 'create_sf_copy': {'required': False, 'type': bool, 'default': True}
        , 'drop_sf_copy': {'required': False, 'type': bool, 'default': True}
        , 'create_db_copy': {'required': False, 'type': bool, 'default': True}
        , 'drop_db_copy': {'required': False, 'type': bool, 'default': True}
    }


def validate_cfg(cfg: dict, arg_map=None, exclude_checks: list=[]) -> dict:

    if arg_map is None:
        arg_map = get_default_arg_map()

    type_defaults = {
        list: []
        , bool: False
        , str: ''
        , dict: {}
    }

    # Check to make sure we are not missing any required args
    missing_required_args = list(set([arg for arg in arg_map if arg_map[arg]['required'] and arg not in exclude_checks]) - set(cfg))
    assert not missing_required_args, f'Missing required arguments {missing_required_args}'

    # This map will store variable names that used the default value, useful for dicts
    used_default_map = {}
    for arg in arg_map:
        # These checks we should excluded
        if arg in exclude_checks:
            continue

        # Add default values to optional missing args
        if arg not in cfg:
            if 'default' in arg_map[arg]:
                cfg[arg] = arg_map[arg]['default']
            else:
                cfg[arg] = type_defaults[arg_map[arg]['type']]
            used_default_map[arg] = True
        
        if not isinstance(cfg[arg], arg_map[arg]['type']):
            raise TypeError(f"ARG {arg} HAS TYPE {type(cfg[arg])} BUT IS EXPECTED TO BE {arg_map[arg]['type']}")

        if isinstance(cfg[arg], dict) or (isinstance(cfg[arg], list) and 'arg_map' in arg_map[arg]):
            
            # Reuse code to validate dicts or lists of dicts, as_dict will convert it back to a dict after
            as_dict = False
            if isinstance(cfg[arg], dict):
                as_dict = True
                cfg[arg] = [cfg[arg]]

            for i, item in enumerate(cfg[arg]):
                # If the default was used and the null_on_default flg is in the arg_map for this var 
                # we can set args value as null and skip the checks validating the keys/values in the arg
                # This usually means the arg was not passed
                if used_default_map.get(arg, False) and arg_map[arg].get('null_on_default', False):
                    cfg[arg][i] = None
                else:

                    # Lower case the keys
                    if arg_map[arg].get('lower_case_keys', False):
                        item = {key.lower(): val for key, val in item.items()}
                    
                    if not arg_map[arg].get('skip_arg_map_check', False):
                        assert 'arg_map' in arg_map[arg], f'arg_map REQUIRED IN arg_map FOR CONFIG VARIABLE {arg}'
                        cfg[arg][i] = validate_cfg(item, arg_map[arg]['arg_map'])
            

            if as_dict:
                cfg[arg] = cfg[arg][0]

    extra_keys = list(set(list(cfg.keys())) - set(list(arg_map.keys())))
    assert not extra_keys, f'UNKNOWN ARGS {extra_keys}'

    return cfg


def update_cfg_from_cfg_tbl(cfg: dict, cfg_tbl_name: str) -> dict:
    # Stored the keys that were passed so we dont override them
    passed_keys = list(cfg.keys())
    cfg = validate_cfg(cfg, exclude_checks=['sf_tbl'])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {cfg_tbl_name} (
            db_tbl STRING,
            sf_tbl STRING,
            arg_name STRING,
            arg_dt STRING,
            arg_val STRING,
            user_id STRING
        )
        USING delta
        PARTITIONED BY (user_id)
    """)

    cfg_df = spark.sql(f"SELECT * FROM {cfg_tbl_name} WHERE LOWER(DB_TBL) = LOWER('{cfg['db_tbl'].strip()}') AND LOWER(USER_ID) = LOWER(CURRENT_USER())")
    if cfg_df.count():
        print(f"Found entry for tbl {cfg['db_tbl']} in cfg table {cfg_tbl_name}")

        default_arg_map = get_default_arg_map()
        for row in cfg_df.collect():
            arg_name, arg_val = row['arg_name'].lower().strip(), row['arg_val']
            if arg_name in ['cfg_json']:
                continue
            elif arg_name not in default_arg_map:
                print(f'Skipping arg {arg_name}, not in default arg map')
            
            arg_dt = default_arg_map[arg_name]['type']
            if arg_dt is not str:
                arg_val = ast.literal_eval(arg_val)
                assert isinstance(arg_val, arg_dt), f'Arg: {arg_name} does not have the expected dt for its value after parse, expected: {arg_dt} got: {type(arg_val)}'

            if arg_name not in cfg or (arg_val != cfg[arg_name] and arg_name not in passed_keys):
                cfg[arg_name] = arg_val
                print(f'Got {arg_name} from cfg tbl with value: {cfg[arg_name]}')
    else:
        print(f"Could not find a config record for {cfg['db_tbl']} in {cfg_tbl_name}")

    return cfg

# def update_cfg_from_cfg_tbl(cfg: dict, cfg_tbl_name: str) -> dict:
#     # Stored the keys that were passed so we dont override them
#     passed_keys = list(cfg.keys())
#     cfg = validate_cfg(cfg)

    # spark.sql(f"""
    #     CREATE TABLE IF NOT EXISTS {cfg_tbl_name} (
    #         db_tbl STRING,
    #         sf_tbl STRING,
    #         primary_keys ARRAY<STRING>,
    #         filter_clause STRING,
    #         exclude_cols ARRAY<STRING>,
    #         db_exclude_cols ARRAY<STRING>,
    #         sf_exclude_cols ARRAY<STRING>,
    #         column_mapping ARRAY<STRUCT<db: STRING, sf: STRING>>,
    #         warehouse_id STRING,
    #         cfg_json STRING,
    #         user_id STRING
    #     ) USING delta
    # """)

#     cfg_df = spark.sql(f"SELECT * FROM {cfg_tbl_name} WHERE LOWER(DB_TBL) = LOWER('{cfg['db_tbl'].strip()}') AND LOWER(USER_ID) = LOWER(CURRENT_USER())")
#     if cfg_df.count():
#         print(f"Found entry for tbl {cfg['db_tbl']} in cfg table {cfg_tbl_name}")

#         df_collected = cfg_df.collect()[0]
#         for arg_name in cfg_df.columns:
#             if arg_name in ('cfg_json', 'user_id'):
#                 continue

#             arg_val = df_collected[arg_name]
#             # Arg should almost always be in cfg because of validate_cfg adding the defaults
#             # Skip any args where the values match or the arg is not using the default value aka passed via user
#             if arg_name not in cfg or (arg_val != cfg[arg_name] and arg_name not in passed_keys):
#                 # Handle array of structs
#                 if isinstance(arg_val, list) and len(arg_val) > 0 and isinstance(arg_val[0], Row):
#                     cfg[arg_name] = [row.asDict() for row in arg_val]
#                 else:
#                     cfg[arg_name] = arg_val

#                 print(f'Got {arg_name} from cfg tbl with value: {cfg[arg_name]}')
        
#         # Any additional_args we want to load from the json string
#         additional_args = ['column_exprs', 'data_type_exprs']

#         if additional_args:
#             cfg_json = json.loads(cfg_df.select(col('cfg_json')).collect()[0][0])
#             for arg_name in additional_args:
#                 # This should only ever happen if we have never seen an arg before
#                 if arg_name not in cfg_json:
#                     continue

#                 arg_val = cfg_json[arg_name]

#                 if arg_name not in cfg or (arg_val != cfg[arg_name] and arg_name not in passed_keys):
#                     cfg[arg_name] = arg_val
#                     print(f'Got {arg_name} from cfg tbl with value: {cfg[arg_name]}')
        
#     else:
#         print(f"Could not find a config record for {cfg['db_tbl']} in {cfg_tbl_name}")

#     return cfg


def update_cfg_tbl(cfg: dict, cfg_tbl_name: str) -> None:
    # print(f'Updating {cfg_tbl_name}')
    # Add the cgf in case we need to extract new fields later
    cfg.update({'cfg_json': json.dumps(cfg)})

    tmp_cfg = [
        {'arg_name': arg_name, 'arg_val': json.dumps(arg_val) if isinstance(arg_val, dict) else str(arg_val)}
        for arg_name, arg_val in cfg.items() 
        if arg_name in [col_name for col_name, col_type in get_save_to_cfg_tbl_args().items() if col_type == 'ROW']
    ]

    (
        spark.createDataFrame(tmp_cfg)
        .withColumn('user_id', current_user())
        .select(lit(cfg['db_tbl']).alias('db_tbl'), lit(cfg['sf_tbl']).alias('sf_tbl'), col('arg_name'), col('arg_val'), current_user().alias('user_id'))
    ).createOrReplaceTempView('validation_config_tmp')

    query = f"""
        MERGE INTO {cfg_tbl_name} a
        USING validation_config_tmp b
        ON a.db_tbl = b.db_tbl and a.user_id = b.user_id and a.arg_name = b.arg_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(query)


# In case we want to convert a cfg value to a row
def extract_cfg_json_arg_to_cfg_row(cfg_tbl_name, arg_lst):
    pivot_query = f"""
        select *
        from (
            select * except(arg_val, arg_name), {', '.join([f'arg_val:{arg_name} as {arg_name}' for arg_name in arg_lst])}
            from {cfg_tbl_name}
            where arg_name = 'cfg_json'
        )
        unpivot (arg_val for arg_name in ({','.join(arg_lst)}))
        where arg_val is not null
    """

    tmp_view_name = 'extract_cfg_json_arg_to_cfg_row__tmp'
    spark.sql(pivot_query).createOrReplaceTempView(tmp_view_name)

    spark.sql(f"""
        MERGE INTO {cfg_tbl_name} AS tgt
        USING {tmp_view_name} AS tmp
        ON tgt.db_tbl = tmp.db_tbl AND tgt.arg_name = tmp.arg_name AND tgt.user_id = tmp.user_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def conv_old_validation_cfg_tbl_to_new(old_tbl_name, replace_old=False):
    new_tbl_name = f'{old_tbl_name}_new'

    old_tbl_df = spark.table(old_tbl_name)
    if 'cfg_json' not in old_tbl_df.columns:
        raise Exception(f'No cfg_json column in {old_tbl_name}, table might already be migrated to new version')
    
    col_exprs = {}
    args_to_save_map = get_save_to_cfg_tbl_args()
    col_dt_map = {col_name: col_dt.lower() for col_name, col_dt in old_tbl_df.dtypes}
    for arg_name, arg_type in args_to_save_map.items():
        if arg_type == 'COL':
            continue
        
        if arg_name not in col_dt_map:
            col_exprs[arg_name] = f'cfg_json:{arg_name} {arg_name}'
        elif col_dt_map[arg_name] == 'string':
            col_exprs[arg_name] = arg_name
        elif col_dt_map[arg_name].startswith('array<'):
            col_exprs[arg_name] = f'to_json({arg_name}) as {arg_name}'
        else:
            raise Exception(f'NOT SURE WHAT TO DO WITH DT: {col_dt_map[arg_name]}')
    
    pivot_query = f"""
        select *
        from (
            select {', '.join(
                [col_name for col_name in args_to_save_map if args_to_save_map[col_name] == 'COL'] 
                +
                list(col_exprs.values())
            )}
            from {old_tbl_name}
        )
        unpivot (arg_val for arg_name in ({','.join(col_exprs.keys())}))
        where arg_val is not null
    """

    pivoted_df = spark.sql(pivot_query)

    (
        pivoted_df
        .select(pivoted_df.db_tbl, pivoted_df.sf_tbl, pivoted_df.arg_name, pivoted_df.arg_val, pivoted_df.user_id)
        .orderBy(['db_tbl', 'user_id', 'arg_name'])
        .write.mode('overwrite').partitionBy('user_id').option('overwriteSchema', 'true').saveAsTable(new_tbl_name)
    )

    # These are the columns we want to extract from the json str that were not columns
    # We could include this above but this function allows us to add more in the future or run it when needed
    # This bit is not needed any more
    # extract_cfg_json_arg_to_cfg_row(new_tbl_name, ['column_exprs', 'data_type_exprs'])

    if replace_old:
        spark.sql(f'DROP TABLE {old_tbl_name}')
        spark.sql(f'ALTER TABLE {new_tbl_name} RENAME TO {old_tbl_name}')
        print(f'Saving new config table to {old_tbl_name}')
    else:
        print(f'Saving new config table to {new_tbl_name}')


#### 1 - Schema check ####
def col_dt_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
    print('Starting check #1, compare the column datatypes')
    result_dict = {}

    # Check using regex because the decimal can have various scales
    sf_db_dt_mappings = [
        {'db': 'bigint', 'sf': 'decimal\(\d+,0\)'}
        , {'db': 'int', 'sf': 'decimal\(\d+,0\)'}
        , {'db': 'double', 'sf': 'decimal\(\d+,2\)'}
        , {'db': 'timestamp_ntz', 'sf': 'timestamp'}
        , {'db': 'decimal\(\d+,2\)', 'sf': 'decimal\(\d+,2\)'}
        , {'db': 'decimal\(\d+,0\)', 'sf': 'decimal\(\d+,0\)'}
    ]
    sf_db_dt_mapping_df = spark.createDataFrame(sf_db_dt_mappings)

    # db_full_tbl_col_info = [['db'] + col for col in get_tbl_col_info(db_tbl_name) if col[0] in col_lst]
    # sf_full_tbl_col_info = [['sf'] + col for col in get_tbl_col_info(sf_tbl_name) if col[0] in col_lst]
    db_full_tbl_col_info = [['db', col['name'], col['data_type']] for col in db_tbl.col_info]
    sf_full_tbl_col_info = [['sf', col['name'], col['data_type']] for col in sf_tbl.col_info]
    schema_df = spark.createDataFrame(db_full_tbl_col_info + sf_full_tbl_col_info, 'src string, col_name string, data_type string')

    schema_res = (
        schema_df.filter("src = 'db'").alias('db')
        .join(schema_df.filter("src = 'sf'").alias('sf'), on=[col('db.col_name') == col('sf.col_name')], how='inner')
        .join(sf_db_dt_mapping_df.alias('mapping'), on=[(nvl(regexp(col('db.data_type'), col('db')), lit(False)))], how='left')
        .select(
            col('db.col_name').alias('column_name'), col('db.data_type').alias('db_dt'), col('sf.data_type').alias('sf_dt')
            , ((nvl(col('db.data_type') == col('sf.data_type'), lit(False))) | (nvl(regexp(col('sf.data_type'), col('sf')), lit(False)))).alias('dt_match_flg')
        )
    )
    
    # Old way of doing it, much slower, leaving this here so the query can be populated in the result, the result should be the same
    # schema_check_query = f"""
    #     SELECT db.column_name, db.data_type db_dt, sf.data_type sf_dt, LOWER(db.data_type) = LOWER(sf.data_type) dt_match_flg
    #     FROM {db_catalog_name}.information_schema.columns db
    #     JOIN {sf_catalog_name}.information_schema.columns sf
    #     ON LOWER(db.column_name) = LOWER(sf.column_name)
    #         AND LOWER(db.table_schema) = '{db_schema_name}' AND LOWER(db.table_name) = '{db_tbl_name}' AND LOWER(db.column_name) in {str(col_lst).replace('[', '(').replace(']', ')')}
    #         AND LOWER(sf.table_schema) = '{sf_schema_name}' AND LOWER(sf.table_name) = '{sf_tbl_name}' AND LOWER(sf.column_name) in {str(col_lst).replace('[', '(').replace(']', ')')}
    # """
    # schema_res = execute_sql_query(schema_check_query, as_df=True)

    if schema_res.select('dt_match_flg').distinct().count() != 1:
        if cfg['fail_at_first_check']:
            raise Exception(f'Schemas do not match')
        result_dict['schema_check'] = {'passed': False}
    else:
        result_dict['schema_check'] = {'passed': True}
    result_dict['schema_check'].update({
        'db_col_count': len(db_full_tbl_col_info)
        , 'sf_col_count': len(sf_full_tbl_col_info)
        , 'missmatched_cols': [(row['column_name'], row['db_dt'], row['sf_dt']) for row in schema_res.filter("not dt_match_flg").collect()]
    })

    # if cfg['include_queries']:
    #     result_dict['schema_check']['query'] = schema_check_query

    return result_dict


#### 2 - Check counts ####
def count_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
    print('Starting check #2, compare the counts of the two tables')
    result_dict = {}

    count_check_query = f"""
        SELECT 'DB_COUNT' src, COUNT(*) row_cnt FROM {db_tbl.full_copy_name}
        UNION ALL
        SELECT 'SF_COUNT', COUNT(*) FROM {sf_tbl.full_copy_name}
    """
    count_res = execute_sql_query(count_check_query, as_df=True)

    count_dict = {row['src']: row['row_cnt'] for row in count_res.collect()}
    if count_dict['DB_COUNT'] != count_dict['SF_COUNT']:
        if cfg['fail_at_first_check']:
            raise Exception(f'Counts do not match. DB: {count_dict["DB_COUNT"]}, SF: {count_dict["SF_COUNT"]}')
        result_dict['count_check'] = {'passed': False}
    else:
        result_dict['count_check'] = {'passed': True}
    result_dict['count_check'].update(count_dict)

    if cfg['include_queries']:
        result_dict['count_check']['query'] = count_check_query
    
    return result_dict


#### 3 - Hash check ####
# if 6 not in cfg['skip_checks'] and cfg['primary_keys'] and cfg['num_of_sample_rows']
# def hash_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
#     print('Starting check #3, take a hash of the entire row and compare the diffrence of the two tables')
#     result_dict = {}

#     cols_csv = db_tbl.get_col_csv()

#     hash_check_query = f"""
#         SELECT COUNT(*)
#         FROM (
#             SELECT sha2(concat_ws('||', {cols_csv}), 256) HASHED FROM {db_tbl.full_copy_name}
#             MINUS
#             SELECT sha2(concat_ws('||', {cols_csv}), 256) FROM {sf_tbl.full_copy_name}
#         )
#     """
#     hash_res = execute_sql_query(hash_check_query)
#     total_row_count = int(hash_res.result.data_array[0][0])

#     if total_row_count != 0:
#         if cfg['fail_at_first_check']:
#             raise Exception(f'Hash check failed, {total_row_count} rows are different')
#         result_dict['hash_check'] = {'passed': False}
#     else:
#         result_dict['hash_check'] = {'passed': True}
#     result_dict['hash_check']['row_diff'] = total_row_count
    
#     if cfg['include_queries']:
#         result_dict['hash_check']['query'] = hash_check_query
    
#     return result_dict

def hash_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
    print('Starting check #3, take a hash of the entire row and compare the diffrence of the two tables')
    result_dict = {}

    cols_csv = db_tbl.get_col_csv()

    db_hashed = f"sha2(concat_ws('||', {cols_csv}), 256) HASHED FROM {db_tbl.full_copy_name}"
    sf_hashed = f"sha2(concat_ws('||', {cols_csv}), 256) HASHED FROM {sf_tbl.full_copy_name}"
    
    hash_check_query = f"SELECT COUNT(*) FROM (SELECT {db_hashed} MINUS SELECT {sf_hashed})"

    hash_res = execute_sql_query(hash_check_query)
    total_row_count = int(hash_res.result.data_array[0][0])

    if total_row_count != 0:
        if cfg['fail_at_first_check']:
            raise Exception(f'Hash check failed, {total_row_count} rows are different')
        result_dict['hash_check'] = {'passed': False}
    else:
        result_dict['hash_check'] = {'passed': True}
    result_dict['hash_check']['row_diff'] = total_row_count
    
    if 6 not in cfg['skip_checks'] and not cfg['primary_keys'] and cfg['num_of_sample_rows'] and not result_dict['hash_check']['passed']:
        print(f"Finding {cfg['num_of_sample_rows']} rows from DB where the hashes did not find a match in SF, saving to {cfg['missmatch_tbl_name']}")
        
        sample_rows_query = f"""CREATE OR REPLACE TABLE {cfg['missmatch_tbl_name']} AS
            WITH 
            DB AS (SELECT *, {db_hashed})
            , SF AS (SELECT *, {sf_hashed})
            SELECT * FROM DB a
            WHERE EXISTS (
                SELECT 1
                FROM (SELECT HASHED FROM DB MINUS SELECT HASHED FROM SF) b
                WHERE a.HASHED = b.HASHED
            )
            LIMIT {cfg['num_of_sample_rows']}
        """
        
        execute_sql_query(sample_rows_query)


    if cfg['include_queries']:
        result_dict['hash_check']['query'] = hash_check_query
    
    return result_dict


#### 4 - Check for duplicate primary keys ####
def duplicate_pk_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
    print('Starting check #4, check for duplicate primary keys')
    result_dict = {}

    duplicate_pk_check_query = f"""
        SELECT lkp_src src, COUNT(DISTINCT pk) as duplicate_key_cnt
        FROM (
            SELECT 'DB_COUNT' as src, concat_ws('||', {','.join(cfg['primary_keys'])}) pk 
            FROM {db_tbl.full_copy_name} GROUP BY 2 HAVING COUNT(*) > 1
            UNION ALL
            SELECT 'SF_COUNT' as src, concat_ws('||', {','.join(cfg['primary_keys'])}) pk 
            FROM {sf_tbl.full_copy_name} GROUP BY 2 HAVING COUNT(*) > 1
        )
        -- Add this lookup so we always have a value, even if its 0
        RIGHT JOIN (SELECT 'DB_COUNT' AS lkp_src UNION ALL SELECT 'SF_COUNT')
        ON src = lkp_src
        GROUP BY 1
    """
    duplicate_pk_res = execute_sql_query(duplicate_pk_check_query, as_df=True)

    duplicate_pk_dict = {row['src']: row['duplicate_key_cnt'] for row in duplicate_pk_res.collect()}
    if duplicate_pk_dict['DB_COUNT'] != 0 or duplicate_pk_dict['SF_COUNT'] != 0:
        if cfg['fail_at_first_check']:
            # raise Exception(f'Duplicate primary keys found, {duplicate_pk_res.manifest.total_row_count} rows are different')
            raise Exception(f'Duplicate primary keys found, DB duplicates: {duplicate_pk_dict["DB_COUNT"]}, SF duplicates: {duplicate_pk_dict["SF_COUNT"]}')
        result_dict['duplicate_pk_check'] = {'passed': False}
    else:
        result_dict['duplicate_pk_check'] = {'passed': True}
    result_dict['duplicate_pk_check'].update(duplicate_pk_dict)

    if cfg['include_queries']:
        result_dict['duplicate_pk_check']['query'] = duplicate_pk_check_query
    return result_dict


#### 5 - Check for rows only in one table based on PKs ####
def exclusive_pk_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
    print('Starting check #5, check for PKs that are exclusive to one table')
    result_dict = {}

    exclusive_pk_check_query = f"""
        SELECT 'SF' as src, count(*) as exclusive_rows
        FROM (
            SELECT * FROM {sf_tbl.full_copy_name} sf WHERE NOT EXISTS (
                SELECT 1
                FROM {db_tbl.full_copy_name} db
                WHERE {' AND '.join([f'sf.{key} <=> db.{key}' for key in cfg['primary_keys']])}
            )
        )
        UNION ALL
        SELECT 'DB' as src, count(*) as exclusive_rows
        FROM (
            SELECT * FROM {db_tbl.full_copy_name} db WHERE NOT EXISTS (
                SELECT 1
                FROM {sf_tbl.full_copy_name} sf
                WHERE {' AND '.join([f'sf.{key} <=> db.{key}' for key in cfg['primary_keys']])}
            )
        )
    """
    exclusive_rows_res = execute_sql_query(exclusive_pk_check_query, as_df=True)

    exclusive_rows_dict = {row['src']: row['exclusive_rows'] for row in exclusive_rows_res.collect()}
    if exclusive_rows_dict['SF'] != 0 or exclusive_rows_dict['DB'] != 0:
        if cfg['fail_at_first_check']:
            raise Exception(f'Rows exclusive to one table based on PKs, SF: {exclusive_rows_dict["SF"]}, DB: {exclusive_rows_dict["DB"]}')
        result_dict['exclusive_pk_check'] = {'passed': False}
    else:
        result_dict['exclusive_pk_check'] = {'passed': True}
    result_dict['exclusive_pk_check'].update(exclusive_rows_dict)

    if cfg['include_queries']:
        result_dict['exclusive_pk_check']['query'] = exclusive_pk_check_query
    
    return result_dict


#### 6
def col_row_level_missmatch_check(cfg: dict, db_tbl: Table, sf_tbl: Table) -> dict:
    print('Starting check #6, check where the PKs match but the non PK columns do not')
    result_dict = {}

    col_lst = db_tbl.get_col_lst()

    # Base query that checks for matches in all columns/rows
    col_pk_row_mismatch_base_query = f"""
        SELECT {','.join(
            [
                f'db.{col} {col}_pk' for col in cfg['primary_keys']
            ] + [
                f'db.{col} {col}_db, sf.{col} {col}_sf, NOT db.{col} <=> sf.{col} {col}_missmatch_flg' for col in col_lst if col not in cfg['primary_keys']
            ]
        )}
        FROM {db_tbl.full_copy_name} db
        JOIN {sf_tbl.full_copy_name} sf
        ON {' AND '.join([f'db.{key} <=> sf.{key}' for key in cfg['primary_keys']])}
            AND ({' OR '.join([f'NOT db.{col} <=> sf.{col}' for col in col_lst if col not in cfg['primary_keys']])})
    """

    # Create a view so that we can cache the results and reuse for the sample rows query
    col_pk_row_mismatch_base_tmp_view_name = f"{cfg['user_schema']}.col_pk_row_mismatch_base__recon_tmp"
    execute_sql_query(f"""CREATE OR REPLACE TABLE {col_pk_row_mismatch_base_tmp_view_name} as {col_pk_row_mismatch_base_query}""")

    # Query to get the number of missmatches in a column
    col_pk_row_mismatch_counts_query = f"""
        SELECT col, count(missmatch_flg) missmatch_cnt
        FROM {col_pk_row_mismatch_base_tmp_view_name}
        UNPIVOT(missmatch_flg for col in ({','.join([f'{col}_missmatch_flg as {col}' for col in col_lst if col not in cfg['primary_keys']])}))
        WHERE missmatch_flg
        GROUP BY 1
    """

    col_pk_row_mismatch_counts_res = execute_sql_query(col_pk_row_mismatch_counts_query, as_df=True)

    col_missmatch_counts_dict = {row['col']: row['missmatch_cnt'] for row in col_pk_row_mismatch_counts_res.collect()}
    if col_pk_row_mismatch_counts_res.count() > 0:
        if cfg['fail_at_first_check']:
            print(json.dumps(col_missmatch_counts_dict, indent=4))
            raise Exception(f'The above {col_pk_row_mismatch_counts_res.count()} columns have missmatches in at least 1 row')
        result_dict['col_pk_row_mismatch'] = {'passed': False}
    else:
        result_dict['col_pk_row_mismatch'] = {'passed': True}
    result_dict['col_pk_row_mismatch']['cols_with_missmatched_rows'] = col_missmatch_counts_dict

    if cfg['include_queries']:
        result_dict['col_pk_row_mismatch']['query'] = col_pk_row_mismatch_counts_query

    # Save sample rows
    if cfg['num_of_sample_rows'] and col_missmatch_counts_dict:
        print(f"Finding {cfg['num_of_sample_rows']} sample rows for each column with missmatches, saving to {cfg['missmatch_tbl_name']}")

        # Query to generate the delta table
        missmatched_rows_query = f"""
            CREATE OR REPLACE TABLE {cfg['missmatch_tbl_name']} AS
        """

        # Old query need to see what is more efficient
        missmatched_rows_query += ' UNION ALL '.join([f"""(
                            SELECT DISTINCT {','.join(
                                        [
                                            f'{key}_pk' for key in cfg['primary_keys']
                                        ] + [
                                            f"'{col}' as col_name", f"{col}_db::STRING db_val", f"{col}_sf::STRING sf_val"
                                        ]
                                    )}
                            FROM {col_pk_row_mismatch_base_tmp_view_name}
                            WHERE {col}_missmatch_flg
                            LIMIT {cfg['num_of_sample_rows']}
                        )""" for col in col_missmatch_counts_dict])

        execute_sql_query(missmatched_rows_query)
    
    execute_sql_query(f"DROP TABLE {col_pk_row_mismatch_base_tmp_view_name}")

    return result_dict


def set_warehouse_id(tmp_wh_id):
    global warehouse_id
    warehouse_id = tmp_wh_id
    return warehouse_id

def set_spark(tmp_cluster_id):
    global spark
    try:
        spark = SparkSession.builder.getOrCreate()
    except:
        print('Failed to get spark session, creating one using databricks-connect')
        assert tmp_cluster_id is not None, 'CLUSTER ID CANNOT BE NONE'
        # Wait for the cluster to start
        print('Waiting for cluster to start...')
        cluster_state = w.clusters.get(tmp_cluster_id).state.value
        if cluster_state == 'TERMINATED':
            w.clusters.start(tmp_cluster_id)
        elif cluster_state != 'RESIZING':
            w.clusters.wait_get_cluster_running(tmp_cluster_id)     
        print('Getting spark session...')   
        spark = DatabricksSession.builder.clusterId(tmp_cluster_id).getOrCreate()
    return spark


def set_globals(warehouse_id=None, cluster_id=None):
    if ('warehouse_id' not in globals() or globals()['warehouse_id'] is None or globals()['warehouse_id'] != warehouse_id) and warehouse_id is not None:
        set_warehouse_id(warehouse_id)
        
    if 'spark' not in globals() or globals()['spark'] is None:
        if 'cluster_id' in globals() and globals()['cluster_id'] is not None:
            cluster_id = globals()['cluster_id']
        set_spark(cluster_id)


def reconcile(cfg: dict) -> dict:
    # Need to do this twice because we need spark
    set_globals()
    print('Starting reconcile...')

    cfg_copy = validate_cfg({key: val for key, val in cfg.items() if key in ('use_cfg_tbl', 'cfg_tbl_name', 'db_tbl')}, exclude_checks=['sf_tbl'])
    if cfg_copy['use_cfg_tbl'] and cfg_copy['cfg_tbl_name']:
        cfg = update_cfg_from_cfg_tbl(cfg, cfg_copy['cfg_tbl_name'])

    cfg = validate_cfg(cfg)
    set_globals(warehouse_id=cfg['warehouse_id'])

    current_user = execute_sql_query("select replace(current_user(), '@dropbox.com', '')", as_df=True).collect()[0][0]
    user_schema = f'users.{current_user}'

    # Test to make sure the schema exists
    execute_sql_query(f'show tables in {user_schema}')

    cfg['user_schema'] = user_schema

    db_tbl = Table(
        cfg['db_tbl'].lower().strip()
        , full_copy_name=f'{user_schema}.%TBL_NAME%__reconcile_tmp_databricks'
        , exclude_cols=cfg['exclude_cols'] + cfg['db_exclude_cols']
        , col_mapping={mapping['db']: mapping['sf'] for mapping in cfg['column_mapping']}
        , column_exprs=cfg['column_exprs']
        , data_type_exprs=cfg['data_type_exprs']
    )

    sf_tbl = Table(
        cfg['sf_tbl'].lower().strip()
        , full_copy_name=f'{user_schema}.%TBL_NAME%__reconcile_tmp_snowflake'
        , exclude_cols=cfg['exclude_cols'] + cfg['sf_exclude_cols']
        , col_mapping={mapping['sf']: mapping['db'] for mapping in cfg['column_mapping']}
        , column_exprs=cfg['column_exprs']
        , data_type_exprs=cfg['data_type_exprs']
    )

    cfg['missmatch_tbl_name'] = f'{user_schema}.{db_tbl.tbl_name}__missmatches'

    db_col_lst = db_tbl.get_col_lst()
    sf_col_lst_alias = sf_tbl.get_col_lst(aliased=True)

    cfg['primary_keys'] = [key.lower() for key in cfg['primary_keys']]

    # Check to make sure primary keys are not excluded and exist in the tbl
    db_missing_pk_keys = list(set(cfg['primary_keys']) - set(db_col_lst))
    sf_missing_pk_keys = list(set(cfg['primary_keys']) - set(sf_col_lst_alias))
    if len(db_missing_pk_keys) != 0 or len(sf_missing_pk_keys) != 0:
        raise Exception(f'Primary keys excluded or does not exists in db or sf table:\nDB missing keys: {db_missing_pk_keys}\nSF missing keys: {sf_missing_pk_keys}')


    # print((set(db_cols) - set(sf_cols)) | (set(sf_cols) - set(db_cols)))
    if set(db_col_lst) != set(sf_col_lst_alias):
        raise Exception(f'THE COLUMNS DO NOT MATCH, DB EXCLUSIVE COLS: {set(db_col_lst) - set(sf_col_lst_alias)}, SF EXCLUSIVE COLS: {set(sf_col_lst_alias) - set(db_col_lst)}')

    # Create a view in delta in case there is a filter clause
    filter_clause = ''
    if cfg['filter_clause']:
        filter_clause = f" WHERE {cfg['filter_clause']}"


    # Copy the sf table to delta so we can query it faster
    if cfg['create_sf_copy']:
        print(f"Creating {cfg['sf_copy_obj_type'].lower()}: {sf_tbl.full_copy_name} for SF table: {sf_tbl.full_tbl_name}")
        execute_sql_query(sf_tbl.get_copy_ddl(cfg['sf_copy_obj_type'], filter_clause=filter_clause, aliased=True))

    # Create a view for the DB table so that we can have a filter
    if cfg['create_db_copy']:
        print(f"Creating {cfg['db_copy_obj_type'].lower()}: {db_tbl.full_copy_name} for DB table: {db_tbl.full_tbl_name}")
        execute_sql_query(db_tbl.get_copy_ddl(cfg['db_copy_obj_type'], filter_clause=filter_clause))


    # Start the validation
    thread_lst = ThreadList()
    result_dict = {}
    if 1 not in cfg['skip_checks']:
        if cfg['use_threads']:
            thread_lst.append(ThreadWithReturnValue(target=col_dt_check, args=(cfg, db_tbl, sf_tbl)))
        else:
            result_dict.update(col_dt_check(cfg, db_tbl, sf_tbl))

    if 2 not in cfg['skip_checks']:
        if cfg['use_threads']:
            thread_lst.append(ThreadWithReturnValue(target=count_check, args=(cfg, db_tbl, sf_tbl)))
        else:
            result_dict.update(count_check(cfg, db_tbl, sf_tbl))

    if 3 not in cfg['skip_checks']:
        if cfg['use_threads']:
            thread_lst.append(ThreadWithReturnValue(target=hash_check, args=(cfg, db_tbl, sf_tbl)))
        else:
            result_dict.update(hash_check(cfg, db_tbl, sf_tbl))


    # Primary key checks
    if cfg['primary_keys']:
        result_dict['primary_keys'] = cfg['primary_keys']

        if 4 not in cfg['skip_checks']:   
            if cfg['use_threads']:
                thread_lst.append(ThreadWithReturnValue(target=duplicate_pk_check, args=(cfg, db_tbl, sf_tbl)))
            else:
                result_dict.update(duplicate_pk_check(cfg, db_tbl, sf_tbl))

        if 5 not in cfg['skip_checks']:
            if cfg['use_threads']:
                thread_lst.append(ThreadWithReturnValue(target=exclusive_pk_check, args=(cfg, db_tbl, sf_tbl)))
            else:
                result_dict.update(exclusive_pk_check(cfg, db_tbl, sf_tbl))

        if 6 not in cfg['skip_checks']:
            if cfg['use_threads']:
                thread_lst.append(ThreadWithReturnValue(target=col_row_level_missmatch_check, args=(cfg, db_tbl, sf_tbl)))
            else:
                result_dict.update(col_row_level_missmatch_check(cfg, db_tbl, sf_tbl))

    if thread_lst:
        thread_lst.start_all_threads()
        result_dict.update(thread_lst.join_all_threads())

    if cfg['drop_sf_copy']:
        print(f"Dropping SF tmp {cfg['sf_copy_obj_type'].lower()}: {sf_tbl.full_copy_name}")
        execute_sql_query(f"DROP {cfg['sf_copy_obj_type']} {sf_tbl.full_copy_name}")

    if cfg['drop_db_copy']:
        print(f"Dropping DB tmp {cfg['db_copy_obj_type'].lower()}: {db_tbl.full_copy_name}")
        execute_sql_query(f"DROP {cfg['db_copy_obj_type']} {db_tbl.full_copy_name}")

    if cfg['cfg_tbl_name'] is not None:
        update_cfg_tbl(cfg, cfg['cfg_tbl_name'])

    return result_dict


def get_tbl_col_counts(full_tbl_name: str, warehouse_id: str, results_tbl: str='users.pamons.tbl_column_counts_tbl') -> None:
    set_globals(warehouse_id)
    
    res = []
    db_tbl = Table(full_tbl_name.lower().strip())

    tbl_row_count = execute_sql_query(f'SELECT COUNT(*) FROM {db_tbl.full_tbl_name}', as_df=True, warehouse_id=warehouse_id).collect()[0][0]

    # Need to do this before the exclude cols
    all_cols_count = f"COUNT(DISTINCT CONCAT_WS('||', {db_tbl.get_col_csv()})) as all_cols_distinct_cnt"

    # You can auto exclude bool cols from being a key, they can only have 3 values: true, false, null
    db_tbl.set_exclude_cols([col['name'] for col in db_tbl.col_info if col['data_type'] == 'boolean'])

    # Search for columns that have unique values
    individual_col_key_query = f"""
        SELECT col_name, distinct_val_count, {tbl_row_count} = distinct_val_count AS is_pk
        FROM (
            SELECT {','.join(
                [all_cols_count]
                +
                [f'COUNT(DISTINCT {col_name}) as {col_name}_distinct_cnt' for col_name in db_tbl.get_col_lst()]
            )}
            FROM {db_tbl.full_tbl_name}
        )
        UNPIVOT(DISTINCT_VAL_COUNT FOR COL_NAME IN ({','.join(
            ['all_cols_distinct_cnt AS all_cols']
            +
            [f'{col_name}_distinct_cnt AS {col_name}' for col_name in db_tbl.get_col_lst()]
        )}))
        ORDER BY distinct_val_count desc
    """
    individual_col_key_df = execute_sql_query(individual_col_key_query, as_df=True, warehouse_id=warehouse_id)

    for row in individual_col_key_df.collect():
        tmp_res = {'tbl_name': db_tbl.full_tbl_name, 'col_name': row['col_name'].lower()}
        tmp_res.update({col: row[col] for col in individual_col_key_df.columns})
        res.append(tmp_res)

    res_df = spark.createDataFrame(res
        , ', '.join(['tbl_name string'] + [' '.join(col) for col in individual_col_key_df.dtypes])
    )
    
    (
        res_df.write.mode('overwrite')
        .option('replaceWhere', f'tbl_name = "{db_tbl.full_tbl_name}"')
        .option('overwriteSchema', True)
        .saveAsTable(results_tbl)
    )

    primary_key_count = res_df.filter(col('is_pk')).count()
    if primary_key_count == 0:
        print(f'Could not find PK for tbl {db_tbl.full_tbl_name}')
    else:
        print(f'Found {primary_key_count} PKs for tbl {db_tbl.full_tbl_name}')

    print(f'Writing results to {results_tbl}')


#### find_primary_keys helper functions ####
def divide_list_into_chunks(l, n): 
    for i in range(0, len(l), n):  
        yield l[i:i + n] 


def test_col_chunk(tbl_name: str, col_chunk: list, exclude_cols: list, warehouse_id: str):
    except_query = f"except({', '.join(exclude_cols + col_chunk)})"
    query = f"""select count(*) from (select * {except_query} from {tbl_name} group by all having count(*) > 1)"""
    return int(execute_sql_query(query, warehouse_id=warehouse_id).result.data_array[0][0])


def find_primary_keys(tbl_name, warehouse_id: str, update_tbl_counts: bool=True, tbl_column_counts_tbl_name: str='users.pamons.tbl_column_counts_tbl'):
    set_globals(warehouse_id)

    if update_tbl_counts:
        get_tbl_col_counts(tbl_name, warehouse_id=warehouse_id, results_tbl=tbl_column_counts_tbl_name)
    
    tbl_res_exist = spark.table(tbl_column_counts_tbl_name) \
        .filter((col('tbl_name') == tbl_name))
    
    if not update_tbl_counts and tbl_res_exist.count() == 0:
        raise Exception(f'update_tbl_counts IS FALSE BUT THERE ARE NO RESULTS IN {tbl_column_counts_tbl_name} FOR TBL {tbl_name}')
    
    # If all the cols combined do not create a PK then it is impossible to create one
    all_cols_pk = tbl_res_exist.filter((col('is_pk')) & (lower(col('col_name')) == 'all_cols'))
    if all_cols_pk.count() == 0:
        print('No PKs possible, all columns combined do no create a PK')
        return []

    res = [row['col_name'] for row in tbl_res_exist.filter((col('is_pk')) & (lower(col('col_name')) != 'all_cols')).collect()]
    if not res:
        non_pk_cols_df = tbl_res_exist.filter(~col('is_pk')).orderBy(asc('distinct_val_count'))

        col_lst = [row['col_name'] for row in non_pk_cols_df.collect()]
        exclude_cols = []
        exclude_cols_old = []

        chunk_size = len(col_lst)//6
        chunk_size = 1 if chunk_size < 1 else chunk_size 
        print(f'Chunk size: {chunk_size}')
        while exclude_cols == exclude_cols_old and len(list(set(col_lst) - set(exclude_cols))):
            exclude_cols_old = exclude_cols.copy()

            for col_chunk in list(divide_list_into_chunks(col_lst, chunk_size)):
                res_count = test_col_chunk(tbl_name, col_chunk, exclude_cols, warehouse_id)
                if res_count == 0:
                    print('Excluding cols:', ', '.join(col_chunk), 'no duplicates')
                    exclude_cols.extend(col_chunk)
                else:
                    for col_name in col_chunk:
                        res_count = test_col_chunk(tbl_name, [col_name], exclude_cols, warehouse_id)
                        if res_count == 0:
                            print(f'Excluding col: {col_name} no duplicates')
                            exclude_cols.append(col_name)

        res = [[col for col in col_lst if col not in exclude_cols]]
    
    return res