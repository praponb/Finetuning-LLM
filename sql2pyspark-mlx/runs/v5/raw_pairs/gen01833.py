from pyspark.sql import functions as F

remaining_keys = (
    df_shipments
    .select("order_id")
    .intersectAll(
        df_shipments
        .filter(F.col("cost") >= 27)
        .select("order_id")
    )
)

verified = (
    df_shipments
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("order_id")
    .agg(
        F.count(F.when(F.col("carrier") == "FedEx", 1)).alias("fedex_rows"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list_2"),
    )
    .orderBy("event_count")
)
