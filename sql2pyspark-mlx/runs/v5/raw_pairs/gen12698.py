from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-07-18"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "status",
        F.concat("carrier", F.lit("/"), "status").alias("carrier_status"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("activity_month"))
    .agg(
        F.count_if(F.col("carrier") == "DHL").alias("dhl_events"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_rows"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
    )
    .filter(F.col("flash_rows") > 1)
    .orderBy(F.col("activity_month").desc_nulls_first())
)
