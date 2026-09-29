from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("avg_qty").desc())

lines = (
    df_carts
    .select("*", F.posexplode_outer("items").alias("pos", "item"))
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "pos",
        F.col("item.product_id"),
        F.col("item.qty"),
        F.col("item.price"),
        F.col("item.promo"),
    )
)

result = (
    lines
    .groupBy("product_id", F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_qty"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("promo") == "VIP", 1)).alias("vip_rows"),
    )
    .withColumn("avg_qty_percent_rank", F.percent_rank().over(w))
)
