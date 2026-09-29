from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.count(F.when(F.col("country") == "JP", 1)).alias("jp_count"),
        (F.max("credit_limit") - F.min("credit_limit")).alias("credit_limit_range"),
    )
    .orderBy(F.col("jp_count").desc())
)
