from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("is_active", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count("age").alias("age_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .orderBy(F.col("signup_month").desc(), F.col("age_count").desc())
)
