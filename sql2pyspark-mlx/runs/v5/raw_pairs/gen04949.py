from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("code"))

result = (
    lines
    .groupBy("customer_id", "code")
    .agg(
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("code") == "WELCOME5", 1)).alias("welcome5_count_2"),
    )
    .orderBy(F.col("spring10_rows").asc_nulls_last())
)
