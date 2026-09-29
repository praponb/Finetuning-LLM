from pyspark.sql import functions as F

exploded = (
    df_price_tiers
    .select("id", "plan", "effective_from", F.explode("tier_limits").alias("element"))
)

cleaned = (
    exploded
    .filter((F.col("effective_from") >= F.lit("2024-07-03").cast("date")) & (F.col("plan") == "basic"))
    .withColumn("element_filled", F.coalesce("element", F.lit(0)))
)

filtered = (
    cleaned
    .filter(F.col("plan").isin(["basic", "pro", "starter"]))
    .withColumn("effective_from_period", F.date_trunc("month", "effective_from"))
)

result = (
    filtered
    .cube(
        F.date_trunc("QUARTER", "effective_from_period").alias("quarter_start"),
        "plan",
    )
    .agg(
        F.sum(F.when(F.col("plan") == "enterprise", F.col("element")).otherwise(0)).alias("enterprise_element"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.sum(F.when(F.col("plan") == "basic", F.col("element_filled"))).alias("basic_total_element_filled"),
        F.count(F.when(F.col("plan") == "pro", 1)).alias("pro_rows"),
        F.sum(F.when(F.col("plan") == "starter", F.col("element")).otherwise(0)).alias("starter_element"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
    )
    .filter(F.col("enterprise_element") >= 0)
)
