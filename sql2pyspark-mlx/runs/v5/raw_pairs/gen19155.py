from pyspark.sql import functions as F

rollup_totals = (
    df_shipments
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .cube(
        F.col("o.channel").alias("channel"),
        F.col("src.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.cost")).alias("avg_cost"),
        F.grouping_id().alias("grouping_level"),
    )
)

result = (
    rollup_totals
    .select(
        "channel",
        "shipment_status",
        "grouping_level",
        F.when(F.col("shipment_status").rlike("^(LOST|DELIVERED)$"), "GROUP_B").when(F.col("shipment_status").rlike("^(IN_TRANSIT|RETURNED)$"), "OTHER_ZONE").otherwise("CORE").alias("shipment_status_zone"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
