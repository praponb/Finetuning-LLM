from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter((~F.col("carrier").eqNullSafe(F.lit("DHL"))) & F.col("order_id").isNotNull())
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

result = (
    valid
    .groupBy(
        "status",
        F.date_trunc("MONTH", "shipped_date").alias("activity_month"),
        "order_id",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("cost").alias("min_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
)
