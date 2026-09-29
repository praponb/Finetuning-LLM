from pyspark.sql import functions as F

with_dims = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("p"), F.col("s.order_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.amount"))
)

result = (
    with_dims
    .groupBy(
        F.date_trunc("week", "shipped_date").alias("week_start"),
        "status",
        F.when(F.col("cost") >= 87, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.count_if(F.col("carrier") == "Flash").alias("flash_events"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_rows"),
        F.avg(F.when(F.col("status") == "DELIVERED", F.col("amount"))).alias("delivered_avg_amount"),
    )
    .orderBy(F.col("in_transit_rows").desc_nulls_last())
)
