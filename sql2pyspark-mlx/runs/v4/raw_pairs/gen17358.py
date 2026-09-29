from pyspark.sql import functions as F

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("carrier").isin(["UPS", "Kerry"]))
    .select("*", F.concat("carrier", F.lit(" "), "status").alias("carrier_status"))
)

enriched = (
    base
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.shipping_fee"), F.col("b.status").alias("order_status"))
)

joined = (
    enriched
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .select("src.*", F.col("o.order_date"), F.col("o.channel"), F.col("o.amount"))
)

result = (
    joined
    .groupBy("channel")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
    )
    .orderBy(F.col("channel").desc())
)
