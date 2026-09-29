from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0)).alias("smb_count"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
    .orderBy(F.col("smb_count").desc(), F.col("signup_year").desc())
)
