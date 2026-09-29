from pyspark.sql import functions as F

by_dimension = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["channel", "shipment_status"], ["channel"], []],
        F.col("o.channel").alias("channel"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.cost")).alias("max_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    by_dimension
    .select("channel", "shipment_status", "num_rows", "max_cost")
    .orderBy(F.col("num_rows").desc())
)
