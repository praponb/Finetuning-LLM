from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
        "status",
    )
    .agg(
        F.avg("discount").alias("avg_discount"),
        F.max("order_date").alias("last_order_date"),
    )
)
