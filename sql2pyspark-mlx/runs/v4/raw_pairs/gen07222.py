from pyspark.sql import functions as F

filtered = (
    df_orders
    .filter((F.col("order_date") >= F.lit("2023-05-19").cast("date")) & F.col("store_id").isNotNull() & F.col("discount").between(0.24, 0.37))
    .withColumn("adjusted_discount", F.round((F.col("discount") * 1.15), 2))
)

result = (
    filtered
    .alias("src")
    .join(df_promotions.alias("pr"), (F.col("src.channel") == F.col("pr.channel")) & F.col("src.order_date").between(F.col("pr.starts_on"), F.col("pr.ends_on")), "inner")
    .select(
        "src.*",
        F.col("pr.id").alias("promo_id"),
        F.col("pr.promo_code"),
        F.col("pr.discount_pct"),
    )
)
