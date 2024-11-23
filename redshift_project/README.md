#### OPTIONAL: If you want to create your own redshift cluster for testing follow the below steps to use terraform to create Redshift clusters in your AWS account:

- add AWS profile and activate in terminal
- change region from main.tf provider block
- run terraform init
`terraform init`
- Run `terraform plan`
this should create 10 resouces:
    - Redshift IAM Role
    - Service Policy
    - Redsihft cluster
    - Internet gateway
    - Route table
    - route table association
    - subnet group
    - security group
    - subnet
    - vpc

- Run `terraform apply` and type yes to confirm creation of resources

- Use the ddl_without_optimization.sql to check what optimizations can be done

#### Assignment 1. Fix Inefficiencies in the Redshift table definitions

Let us learn about how Redshift databases can be optimized. Key issues for the assignment:
Suboptimal distribution: Use DISTSTYLE ALL or EVEN where KEY was appropriate.
Irrelevant sort keys: Chose columns unlikely to improve query performance.
Excessively large or incorrect data types: Inflating data storage and processing time.
Removed constraints: No primary keys or foreign keys for integrity checks.

This has several inefficiencies, such as:

- Suboptimal or missing distribution styles: Using ALL distribution or no distribution keys, leading to data duplication or poor parallelism.
- Missing or irrelevant sort keys: Removing sort keys or adding ones that don’t align with query patterns.
- Incorrect data types: Using unnecessarily large data types or mismatched data types for the content.
- Redundant columns: Adding unnecessary or duplicate columns.
- Lack of constraints: Omitting primary keys and foreign keys.

