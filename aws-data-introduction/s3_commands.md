- In this lab you will learn about AWS S3 bucket properties
- Learn to use AWS GUI and Cloudshell
`bucket_suffix=$(aws sts get-caller-identity --query Account --output text | md5sum | awk '{print substr($1,length($1)-5)}')`

`account=$(aws sts get-caller-identity | jq -r '.Account')`

`echo $bucket_suffix`

`echo $account`

`aws s3api create-bucket --region us-west-2 --bucket data-eng-with-aws-$bucket_suffix  --create-bucket-configuration LocationConstraint=us-west-2`

`aws s3 ls`

`aws s3 cp --recursive --copy-props none s3://aws-dataengineering-day.workshop.aws/data/ s3://data-eng-with-aws/tickets/`

