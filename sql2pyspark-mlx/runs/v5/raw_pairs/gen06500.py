from pyspark.sql import functions as F

dim_totals = (
    df_order_items
    .alias("src")
    .join(df_orders.alias("dim"), F.col("src.order_id") == F.col("dim.id"), "inner")
    .groupBy(F.col("dim.channel").alias("channel"), F.col("dim.status").alias("status"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("src.unit_price")).alias("max_unit_price"),
    )
)

result = (
    dim_totals
    .select(
        "channel",
        "status",
        "max_unit_price",
        F.when(F.col("status").rlike("^(CANCELLED|COMPLETED)$"), "PRIORITY").when(F.col("status").rlike("^(PENDING|SHIPPED)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("max_unit_price").desc_nulls_first(), "status")
)
