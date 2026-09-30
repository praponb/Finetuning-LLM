from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-03-06").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "status",
        F.concat_ws("/", "carrier", "status").alias("carrier_status_key"),
    )
)

result = (
    cleaned
    .groupBy("order_id")
    .agg(
        F.count_if(F.col("carrier") == "FedEx").alias("fedex_events"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)
