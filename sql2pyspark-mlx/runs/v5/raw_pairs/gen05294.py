from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["order_status", "shipment_status"], ["order_status"], []],
        F.col("d.status").alias("order_status"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.cost")).alias("min_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    dim_totals
    .select("order_status", "shipment_status", "num_rows", "min_cost")
    .orderBy(F.col("order_status").asc_nulls_first())
)
