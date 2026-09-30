from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("key", "customer_id").orderBy("coupon_codes_count_2")
w_ordered = Window.orderBy(F.col("coupon_codes_count_2").desc())

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

cleaned = (
    exploded
    .filter(F.col("key").isNotNull())
    .select(
        "*",
        F.size("coupon_codes").alias("coupon_codes_count"),
        F.size("coupon_codes").alias("coupon_codes_count_2"),
    )
)

scored = (
    cleaned
    .withColumn("coupon_codes_count_2_rank", F.rank().over(w))
    .withColumn("coupon_codes_count_2_percent_rank", F.percent_rank().over(w_ordered))
)

result = (
    scored
    .groupBy(
        "key",
        F.when(F.col("coupon_codes_count") >= 66, "PREMIUM").otherwise("STANDARD").alias("coupon_codes_count_tier"),
    )
    .agg(
        F.count(F.when(F.col("key") == "weight_kg", 1)).alias("weight_kg_count"),
        F.sort_array(F.collect_set("key")).alias("key_list"),
    )
)
