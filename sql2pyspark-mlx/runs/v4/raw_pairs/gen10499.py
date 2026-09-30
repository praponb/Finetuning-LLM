from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("is_active") & (F.col("signup_date") < "2024-02-01")) | (F.col("city") == "Bangkok"))
    .groupBy("city")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.sum(F.when(F.col("country") == "SG", F.col("credit_limit")).otherwise(0)).alias("sg_credit_limit"),
    )
    .filter(F.col("sg_credit_limit") < 72)
    .drop("sg_credit_limit")
)
