from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("code"))

filtered = (
    lines
    .filter(F.col("code").isNotNull())
    .withColumn("code_prefix", F.substring("code", 1, 2))
    .withColumn("resolved_code", F.coalesce("code", F.lit("N/A")))
)

cleaned = (
    filtered
    .filter(F.col("code").isNotNull() & (F.col("code") == "SPRING10"))
    .select(
        "*",
        F.when(F.col("code").isin(["WELCOME5", "BUNDLE"]), "Core").otherwise("Other").alias("code_group"),
    )
)

cleaned_carts = (
    cleaned
    .filter(F.col("code") == "SPRING10")
    .select(
        "id",
        "code",
        "code_prefix",
        "resolved_code",
        "code_group",
        F.initcap(F.trim("code")).alias("code_title"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("resolved_code_2"),
    )
)

cleaned_carts_2 = (
    cleaned_carts
    .filter(F.col("code").isNotNull())
    .withColumn("code_group_2", F.when(F.col("code").isin(["WELCOME5", "BUNDLE"]), "Group A").otherwise("Other"))
)

base = (
    cleaned_carts_2
    .filter(F.col("code").isNotNull() & (~F.col("code").eqNullSafe(F.lit("VIP"))))
    .select(
        "id",
        "code",
        "code_prefix",
        "code_title",
        "resolved_code_2",
        "code_group_2",
        F.lower("code").alias("code_lower"),
        F.length("code").alias("code_length"),
    )
)

result = (
    base
    .groupBy("code")
    .agg(
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("code")).alias("code_list"),
    )
    .orderBy(F.col("code").desc_nulls_last())
)
