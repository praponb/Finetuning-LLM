from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

result = (
    flattened
    .groupBy("tag")
    .agg(
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_count"),
        F.count("*").alias("event_count"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.count_if(F.col("tag") == "VIP").alias("vip_events"),
        F.count(F.when(F.col("tag") == "BUNDLE", 1)).alias("bundle_count"),
        F.size(F.collect_set("tag")).alias("num_tags_2"),
    )
    .filter(F.col("num_tags") > 1)
    .orderBy(F.col("spring10_count").desc_nulls_first())
)
