from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date") <= "2024-01-01")
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .filter(F.col("avg_discount") < 0.16)
)
