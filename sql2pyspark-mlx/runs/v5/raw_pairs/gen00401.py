from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(6000, 13100))
    .groupBy("segment", "city")
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit")).otherwise(0)).alias("smb_credit_limit"),
    )
    .orderBy("city")
)
