from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "left")
    .cube(
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.cost")).alias("total_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    dim_totals
    .select(
        "channel",
        "order_status",
        "shipment_status",
        "total_cost",
        "grouping_level",
        F.when(F.col("shipment_status").rlike("^(LOST|RETURNED)$"), "EXTENDED").otherwise("PRIORITY").alias("shipment_status_zone"),
    )
    .orderBy("channel")
)
