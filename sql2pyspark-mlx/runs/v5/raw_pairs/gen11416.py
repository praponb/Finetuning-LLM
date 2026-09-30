from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "is_active",
        F.when(F.col("age") >= 65, "high").when(F.col("age") >= 31, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
)
