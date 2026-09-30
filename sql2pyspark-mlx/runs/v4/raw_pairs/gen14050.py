from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "shipped_date"))
    .rollup("activity_month", "year_start")
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
