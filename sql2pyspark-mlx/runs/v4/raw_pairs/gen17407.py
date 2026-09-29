from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "customer_id", "created_at", F.explode("coupon_codes").alias("code"))
)

result = (
    flattened
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"), "customer_id")
    .agg(
        F.max("created_at").alias("last_created_at"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
        F.countDistinct("code").alias("distinct_codes"),
    )
    .orderBy(F.col("hour_window").desc())
)
