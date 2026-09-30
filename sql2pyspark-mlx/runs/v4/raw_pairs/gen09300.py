from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("value"))

valid = (
    flattened
    .filter(F.col("value").isin(["BUNDLE", "SPRING10"]))
    .select(
        "*",
        F.coalesce("value", F.lit("N/A")).alias("value_filled"),
        F.coalesce("value", F.lit("DIRECT")).alias("resolved_value"),
    )
)

valid_carts = (
    valid
    .filter(F.col("value") == "BUNDLE")
    .withColumn("value_filled_2", F.coalesce("value", F.lit("N/A")))
    .withColumn("value_lower", F.lower("value"))
    .withColumn("value_filled_3", F.coalesce("value", F.lit("N/A")))
)

cleaned = (
    valid_carts
    .filter(F.col("value") != "WELCOME5")
    .select("*", F.trim("value").alias("clean_value"))
)

result = (
    cleaned
    .select(
        "id",
        "value",
        "value_filled",
        "resolved_value",
        F.when(F.col("value").rlike("^(WELCOME5|VIP)$"), "CORE").when(F.col("value").rlike("^(SPRING10|BUNDLE)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("value_zone"),
    )
    .orderBy(F.col("value_zone").asc_nulls_first())
)
