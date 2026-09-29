from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("value"))

result = (
    exploded
    .groupBy("customer_id")
    .agg(
        F.count_if(F.col("value") == "BUNDLE").alias("bundle_events"),
        F.count(F.when(F.col("value") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("value") == "VIP", 1)).alias("vip_rows_2"),
        F.count(F.when(F.col("value") == "VIP", 1)).alias("vip_rows_3"),
    )
    .orderBy(F.col("vip_rows_2").asc_nulls_first())
)
