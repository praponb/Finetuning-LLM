from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("tag"))

result = (
    flattened
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("tag") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.size(F.collect_set("tag")).alias("num_tags"),
    )
    .orderBy(F.col("welcome5_rows").asc_nulls_first())
)
