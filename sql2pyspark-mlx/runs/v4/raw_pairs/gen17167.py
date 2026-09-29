from pyspark.sql import functions as F

parsed = (
    df_carts
    .select(
        "id",
        "coupon_codes",
        "ship_to",
        F.size(F.filter("items", lambda x: x["qty"] >= 3)).alias("bulk_lines"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.coalesce(F.col("attributes")["device"], F.lit("unknown")).alias("device_or_default"),
    )
)

result = (
    parsed
    .groupBy(
        F.when(F.col("bulk_lines") >= 3, "LARGE").otherwise("SMALL").alias("bulk_lines_tier"),
    )
    .agg(
        F.round(F.avg("bulk_lines"), 2).alias("avg_bulk_lines"),
        F.count("*").alias("event_count"),
    )
    .orderBy("bulk_lines_tier")
)
