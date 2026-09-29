from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("element").orderBy("is_vip")
w_hist = Window.partitionBy("element")
w3 = Window.orderBy(F.col("is_vip").desc())

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("element"))

valid = (
    flattened
    .filter(F.col("element") == "BUNDLE")
    .withColumn("element_group", F.when(F.col("element").isin(["BUNDLE", "WELCOME5"]), "Tier 1").otherwise("Other"))
    .withColumn("is_vip", F.when(F.col("element") == "VIP", 1).otherwise(0))
    .withColumn("element_filled", F.coalesce("element", F.lit("NONE")))
)

metrics = (
    valid
    .withColumn("is_vip_rank", F.rank().over(w))
    .withColumn("element_max_is_vip", F.max("is_vip").over(w_hist))
    .withColumn("element_min_is_vip", F.min("is_vip").over(w_hist))
    .withColumn("element_min_is_vip_2", F.min("is_vip").over(w_hist))
    .withColumn("is_vip_rank_2", F.rank().over(w3))
)

result = (
    metrics
    .cube("element")
    .agg(
        F.percentile_approx("is_vip", 0.95).alias("is_vip_p95"),
        F.percentile_approx("is_vip_rank", [0.25, 0.5, 0.75]).alias("is_vip_rank_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("element").asc_nulls_last())
)
