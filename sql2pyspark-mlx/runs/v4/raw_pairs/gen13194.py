from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-09-13").cast("date")) & F.col("order_id").isNotNull())
    .withColumn("cost_band", F.when(F.col("cost") >= 78, "High").when(F.col("cost") >= 41, "Medium").otherwise("Low"))
    .withColumn("shipped_date_minus_67d", F.date_sub("shipped_date", 67))
)

result = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.discount"), F.col("p.channel"), F.col("p.shipping_fee"))
)
