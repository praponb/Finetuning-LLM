from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-09-01").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
    )
)

result = (
    cleaned
    .groupBy(F.when(F.col("cost") >= 74, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count_if(F.col("status") == "DELIVERED").alias("delivered_events"),
    )
)
