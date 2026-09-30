from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("cost") <= 46) & (F.col("order_id") == 29))
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

summary = (
    valid_shipments
    .groupBy(
        "status",
        F.when(F.year("delivered_date") < 2024, "before_2024").when(F.year("delivered_date") == 2024, "2024").otherwise("after_2024").alias("delivered_cohort"),
    )
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    summary
    .filter(F.col("cost_count") >= 3)
    .orderBy("status", "delivered_cohort")
    .limit(50)
)
