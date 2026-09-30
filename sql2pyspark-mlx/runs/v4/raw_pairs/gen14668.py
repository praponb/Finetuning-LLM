from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("hour_window").orderBy(F.col("bundle_total_price").desc())

flattened = (
    df_carts
    .withColumn("line", F.explode("items"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        F.col("line.product_id"),
        F.col("line.qty"),
        F.col("line.price"),
        F.col("line.promo"),
    )
)

result = (
    flattened
    .groupBy(
        F.date_trunc("HOUR", "created_at").alias("hour_window"),
        F.when(F.col("price") >= 211, "HIGH").otherwise("LOW").alias("price_tier"),
        F.when(F.col("qty") >= 4, "PREMIUM").otherwise("STANDARD").alias("qty_tier"),
    )
    .agg(
        F.count(F.when(F.col("promo") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.percentile_approx("product_id", 0.5).alias("product_id_p50"),
        F.sum(F.when(F.col("promo") == "BUNDLE", F.col("price"))).alias("bundle_total_price"),
        F.percentile_approx("product_id", 0.5).alias("product_id_p50_2"),
        F.count("*").alias("cnt"),
    )
    .withColumn("bundle_total_price_rank", F.rank().over(w))
    .orderBy(F.col("welcome5_rows").desc_nulls_last())
)
