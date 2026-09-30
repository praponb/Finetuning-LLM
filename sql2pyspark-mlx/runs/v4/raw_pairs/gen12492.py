from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    exploded
    .groupBy("code")
    .agg(
        F.count(F.when(F.col("code") == "VIP", 1)).alias("vip_count"),
        F.count("*").alias("num_rows"),
        F.countDistinct("code").alias("distinct_codes"),
        F.count_if(F.col("code") == "VIP").alias("vip_events"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .orderBy(F.col("vip_events").desc_nulls_last())
)
