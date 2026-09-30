from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

result = (
    exploded
    .groupBy("code", "customer_id")
    .agg(
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_rows"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_rows"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
    )
)
