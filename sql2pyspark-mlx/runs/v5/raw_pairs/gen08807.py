from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-04-29"))) & (F.col("cost") >= 79))
    .withColumn("status_carrier", F.concat("status", F.lit(" - "), "carrier"))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("UNKNOWN")))
)

result = (
    filtered
    .alias("s")
    .join(df_orders.alias("b"), F.col("s.order_id") == F.col("b.id"), "inner")
    .filter(F.col("b.store_id").isNotNull())
    .select("s.*", F.col("b.order_date"), F.col("b.discount"))
)
