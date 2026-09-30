from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

result = (
    lines
    .groupBy("customer_id", "tag")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("tag").alias("distinct_tags"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count"),
        F.count(F.when(F.col("tag") == "VIP", 1)).alias("vip_count_2"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.sort_array(F.collect_set("tag")).alias("tag_list_2"),
    )
    .orderBy(F.col("vip_count").desc())
)
