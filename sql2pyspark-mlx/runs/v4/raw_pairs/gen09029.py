from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active")
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.sum(F.when(F.col("country") == "UK", F.col("credit_limit")).otherwise(0)).alias("uk_credit_limit"),
        F.min("credit_limit").alias("min_credit_limit"),
    )
    .filter(F.col("min_credit_limit") > 14300)
    .drop("min_credit_limit")
    .orderBy("uk_credit_limit")
)
