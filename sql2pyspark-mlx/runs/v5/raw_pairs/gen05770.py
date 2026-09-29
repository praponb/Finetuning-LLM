from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode_outer("coupon_codes").alias("tag"))

result = (
    flattened
    .groupBy("tag", F.date_trunc("hour", "created_at").alias("hour_window"))
    .agg(
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
    )
    .orderBy("vip_count")
)
