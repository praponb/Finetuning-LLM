from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("delivered_date") > F.lit("2023-02-28").cast("date")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.when(F.col("cost") >= 74, "High").when(F.col("cost") >= 23, "Medium").otherwise("Low").alias("cost_tier"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_filled"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_orders.alias("d"), F.col("src.order_id") == F.col("d.id"), "inner")
    .rollup(
        F.col("d.status").alias("order_status"),
        F.col("src.carrier").alias("carrier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("src.cost")).alias("min_cost"),
    )
    .orderBy(F.col("num_rows").asc_nulls_last())
)
