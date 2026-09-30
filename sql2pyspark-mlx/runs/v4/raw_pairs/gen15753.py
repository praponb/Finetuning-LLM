from pyspark.sql import functions as F

flattened = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("coupon_codes").alias("code"),
    )
)

result = (
    flattened
    .groupBy("code", F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.min("created_at").alias("first_created_at"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_count"),
    )
    .filter(F.col("welcome5_count") > 1)
)
