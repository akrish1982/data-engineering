- create crawlers
- create databases
- run crawlers
- edit table person column names:
id, full_name, last_name and first_name.
- create visual ETL
    - sport_team - change id to double data type
    - sport_location - no transform
    - sporting_event - column “start_date_time” => TIMESTAMP and “start_date” => DATE
    - sporting_event_ticket column “id” => DOUBLE, “sporting_event_id” => DOUBLE, “ticketholder_id” => DOUBLE
    - person tables - column “id” => DOUBLE