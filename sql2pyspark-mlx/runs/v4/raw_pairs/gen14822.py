from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-07-01").cast("date")) & F.col("carrier").isNotNull() & F.month("shipped_date").isin([6, 7, 8]))
    .select(
        "id",
        "order_id",
        "carrier",
        "cost",
        "status",
        F.length("status").alias("status_length"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_orders.alias("d"), F.col("a.order_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.channel"), F.col("d.amount"))
)
