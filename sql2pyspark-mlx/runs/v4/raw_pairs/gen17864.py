from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") <= "2025-08-01") & (F.col("age") < 66))
    .groupBy(F.quarter("signup_date").alias("signup_quarter"))
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .orderBy(F.col("signup_quarter").desc())
)
