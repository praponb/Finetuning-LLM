from pyspark.sql import functions as F

rollup_totals = (
    df_orders
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.region").alias("region"),
        F.col("p.city").alias("city"),
        F.col("e.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.shipping_fee")).alias("total_shipping_fee"),
        F.min(F.col("e.amount")).alias("min_amount"),
    )
)

result = (
    rollup_totals
    .select(
        "region",
        "city",
        "status",
        "rows_total",
        "total_shipping_fee",
        "min_amount",
    )
    .orderBy("region", "status")
)
