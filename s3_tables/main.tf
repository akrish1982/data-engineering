resource "aws_s3tables_table_bucket" "mys3testtablebucket" {
  name = "s3-test-table-bucket"
}

resource "aws_s3tables_namespace" "stocks" {
  namespace        = "stocks"
  table_bucket_arn = aws_s3tables_table_bucket.mys3testtablebucket.arn
}

resource "aws_s3tables_table" "stockbasics" {
  name             = "stockbasics"
  namespace        = aws_s3tables_namespace.stocks.namespace
  table_bucket_arn = aws_s3tables_namespace.stocks.table_bucket_arn
  format           = "ICEBERG"
}