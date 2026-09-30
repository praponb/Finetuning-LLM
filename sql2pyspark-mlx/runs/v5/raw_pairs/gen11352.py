from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("key", "value"),
    )
)

flattened = lines.select("value", F.explode("coupon_codes").alias("tag"))

valid = (
    flattened
    .filter(F.col("tag").isNotNull() & (F.col("tag") == "BUNDLE"))
    .withColumn("tag_group", F.when(F.col("tag").isin(["VIP", "SPRING10"]), "Group A").otherwise("Other"))
    .withColumn("tag_upper", F.upper("tag"))
)

base = (
    valid
    .filter(F.col("tag").isNotNull() & F.col("tag").isNotNull())
    .select("*", F.substring("tag", 1, 4).alias("tag_prefix"))
)

result = (
    base
    .cube("tag")
    .agg(
        F.count_if(F.col("tag") == "WELCOME5").alias("welcome5_events"),
        F.size(F.collect_set("tag")).alias("num_tags"),
        F.sort_array(F.collect_set("tag")).alias("tag_list"),
        F.grouping("tag").alias("grouping_level"),
    )
    .orderBy(F.col("welcome5_events").asc_nulls_last())
)
