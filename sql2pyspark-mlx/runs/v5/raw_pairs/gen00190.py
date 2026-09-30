from pyspark.sql import functions as F

exploded = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

valid = (
    exploded
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        F.length("tag").alias("tag_length"),
        F.initcap(F.trim("tag")).alias("tag_title"),
        F.coalesce("tag", F.lit("OTHER")).alias("tag_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("tag").isin(["SPRING10", "WELCOME5"]))
    .select(
        "customer_id",
        "tag",
        "tag_length",
        "tag_title",
        "tag_clean",
        F.when(F.col("tag_length") >= 86, "High").when(F.col("tag_length") >= 33, "Medium").otherwise("Low").alias("tag_length_level"),
    )
)

base = (
    cleaned
    .filter(F.col("customer_id") == 26)
    .select(
        "tag",
        "tag_length",
        "tag_title",
        "tag_clean",
        F.floor("tag_length").alias("tag_length_whole"),
        F.coalesce("tag", F.lit("NONE")).alias("tag_clean_2"),
    )
)

result = (
    base
    .select(
        "tag",
        "tag_length",
        "tag_title",
        "tag_clean",
        "tag_length_whole",
        "tag_clean_2",
        F.when(F.col("tag").rlike("^(WELCOME5|VIP)$"), "GROUP_B").otherwise("PRIORITY").alias("tag_zone"),
    )
    .orderBy(F.col("tag").desc_nulls_first())
)
