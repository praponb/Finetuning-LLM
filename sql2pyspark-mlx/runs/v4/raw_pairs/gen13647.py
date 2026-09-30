from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("price_per_bundle").desc())

exploded = df_carts.select("id", "customer_id", "coupon_codes", F.inline("items"))

result = (
    exploded
    .groupBy(
        "product_id",
        F.when(F.col("price") >= 245, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
        F.when(F.col("qty") >= 4, "HIGH").otherwise("LOW").alias("qty_tier"),
    )
    .agg(
        F.round(F.sum("product_id") / F.nullif(F.count(F.when(F.col("promo") == "WELCOME5", 1)), F.lit(0)), 2).alias("product_id_per_welcome5"),
        F.round(F.sum("price") / F.nullif(F.count(F.when(F.col("promo") == "BUNDLE", 1)), F.lit(0)), 2).alias("price_per_bundle"),
    )
    .withColumn("price_per_bundle_percent_rank", F.percent_rank().over(w))
)
