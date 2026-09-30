from pyspark.sql import functions as F

flattened = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

result = (
    flattened
    .cube("code")
    .agg(
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_count"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("spring10_count").desc_nulls_first())
)
