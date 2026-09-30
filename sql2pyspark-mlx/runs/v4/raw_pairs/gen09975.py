from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

cleaned = (
    lines
    .filter(F.col("value").isin(["VIP", "SPRING10"]))
    .withColumn("value_group", F.when(F.col("value").isin(["SPRING10", "VIP"]), "Group A").otherwise("Other"))
    .withColumn("value_filled", F.coalesce("value", F.lit("UNKNOWN")))
)

valid = (
    cleaned
    .filter(F.col("value") == "SPRING10")
    .select("*", F.initcap(F.trim("value")).alias("value_title"))
)

cleaned_carts = (
    valid
    .filter(F.col("value").isNotNull() & F.col("value").isNull())
    .select("id", "value", "value_filled", F.upper("value").alias("value_upper"))
)

valid_carts = (
    cleaned_carts
    .filter(F.col("value").isNotNull())
    .withColumn("value_filled_2", F.coalesce("value", F.lit("Unknown")))
    .withColumn("resolved_value", F.coalesce("value", F.lit("NONE")))
)

base = (
    valid_carts
    .filter(F.col("value").isNotNull())
    .withColumn("value_group", F.when(F.col("value").isin(["SPRING10", "WELCOME5"]), "Group A").otherwise("Other"))
)

valid_carts_2 = (
    base
    .filter(F.col("value") == "WELCOME5")
    .select(
        "id",
        "value",
        "value_filled",
        "value_filled_2",
        "resolved_value",
        "value_group",
        F.length("value").alias("value_length"),
    )
)

result = (
    valid_carts_2
    .groupBy(
        F.when(F.col("value_length") >= 87, "LARGE").otherwise("SMALL").alias("value_length_tier"),
    )
    .agg(
        F.max(F.when(F.col("value") == "VIP", F.col("value_length"))).alias("vip_max_value_length"),
        F.count("*").alias("cnt"),
        F.round(F.avg("value_length"), 2).alias("avg_value_length"),
    )
)
