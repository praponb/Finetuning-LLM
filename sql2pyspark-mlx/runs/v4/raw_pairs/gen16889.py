from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode("coupon_codes").alias("code"))

valid = (
    lines
    .filter(F.col("code").isNotNull() & (F.col("code") == "VIP"))
    .withColumn("code_group", F.when(F.col("code").isin(["WELCOME5", "BUNDLE"]), "Priority").otherwise("Other"))
)

valid_carts = (
    valid
    .filter(F.col("code") != "SPRING10")
    .select(
        "*",
        F.substring("code", 1, 4).alias("code_prefix"),
        F.coalesce("code", F.lit("OTHER")).alias("code_filled"),
    )
)

filtered = (
    valid_carts
    .filter(F.col("code").isNotNull())
    .select(
        "code_group",
        "code_filled",
        F.upper("code").alias("code_upper"),
        F.when(F.col("code").isin(["BUNDLE", "WELCOME5"]), "Core").otherwise("Other").alias("code_group_2"),
        F.coalesce("code", F.lit("UNKNOWN")).alias("code_filled_2"),
    )
)

result = (
    filtered
    .select("code_group", "code_filled", "code_upper", "code_group_2", "code_filled_2")
)
