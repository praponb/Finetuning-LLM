from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Chaiya%"))
    .agg(
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
    )
)
