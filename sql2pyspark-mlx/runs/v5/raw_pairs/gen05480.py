from pyspark.sql import functions as F

dim_totals = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("o"), F.col("e.order_id") == F.col("o.id"), "inner")
    .rollup(
        F.col("o.channel").alias("channel"),
        F.col("e.status").alias("shipment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("e.cost")).alias("min_cost"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select(
        "channel",
        "shipment_status",
        "num_rows",
        "min_cost",
        "level_id",
        F.when(F.col("shipment_status").rlike("^(LOST|RETURNED)$"), "OTHER_ZONE").when(F.col("shipment_status").rlike("^(IN_TRANSIT|DELIVERED)$"), "GROUP_B").otherwise("EXTENDED").alias("shipment_status_zone"),
    )
    .orderBy(F.col("shipment_status").desc_nulls_last())
)
