from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
        F.when(F.col("cost") >= 77, "high").when(F.col("cost") >= 17, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
    .filter(F.col("delivered_count") > 74)
    .orderBy(F.col("delivered_count").desc(), "shipped_cohort", "cost_bucket")
    .limit(5)
)
