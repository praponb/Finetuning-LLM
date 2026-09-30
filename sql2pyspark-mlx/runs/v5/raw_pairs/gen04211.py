from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-06-28"))) & (F.col("shipped_date") >= "2025-09-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "status",
        F.date_add("delivered_date", 47).alias("delivered_date_plus_47d"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

result = (
    cleaned
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count(F.when(F.col("status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.count("*").alias("cnt"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
    .filter(F.col("cnt") >= 1)
)
