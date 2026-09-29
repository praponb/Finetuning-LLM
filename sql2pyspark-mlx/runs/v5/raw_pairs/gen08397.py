from pyspark.sql import functions as F

lines = df_price_tiers.select("id", "plan", F.explode("discounts").alias("element"))

filtered = (
    lines
    .filter(~F.col("plan").isin(["enterprise", "pro"]))
    .withColumn("element_filled", F.coalesce("element", F.lit(0)))
)

filtered_price_tiers = (
    filtered
    .filter(F.col("element_filled") <= 35)
    .select("*", F.col("element").cast("int").alias("element_int"))
)

valid = (
    filtered_price_tiers
    .filter(F.col("element_filled") > 48)
    .select(
        "id",
        "plan",
        "element",
        "element_int",
        F.length("plan").alias("plan_length"),
        (F.col("element_filled") * F.col("element_int")).alias("element_filled_times_element_int"),
    )
)

result = (
    valid
    .groupBy("plan")
    .agg(
        F.percentile_approx("element", [0.25, 0.5, 0.75]).alias("element_quartiles"),
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
