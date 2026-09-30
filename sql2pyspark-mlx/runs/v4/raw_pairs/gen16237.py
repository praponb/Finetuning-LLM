from pyspark.sql import functions as F

flattened = (
    df_carts
    .select("id", "created_at", F.explode_outer("coupon_codes").alias("element"))
)

cleaned = (
    flattened
    .filter((F.col("created_at") >= F.lit("2025-03-01 21:00:00").cast("timestamp")) & F.col("element").isNotNull() & F.col("element").isin(["WELCOME5", "SPRING10"]))
    .withColumn("element_lower", F.lower("element"))
    .withColumn("element_title", F.initcap(F.trim("element")))
    .withColumn("element_filled", F.coalesce("element", F.lit("N/A")))
)

base = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "created_at",
        "element",
        "element_lower",
        "element_title",
        "element_filled",
        F.coalesce("element", F.lit("unassigned")).alias("element_filled_2"),
        F.trim("element").alias("clean_element"),
    )
)

cleaned_carts = (
    base
    .filter(F.col("element").isNotNull())
    .withColumn("created_at_month", F.month("created_at"))
    .withColumn("created_at_hour", F.hour("created_at"))
    .withColumn("resolved_element", F.coalesce("element", F.lit("N/A")))
)

valid = (
    cleaned_carts
    .filter((F.col("created_at") > F.lit("2025-03-02 10:00:00").cast("timestamp")) & F.col("element").isNotNull())
    .select(
        "id",
        "created_at",
        "element",
        "element_title",
        "element_filled",
        "element_filled_2",
        "created_at_month",
        "created_at_hour",
        "resolved_element",
        F.upper("element").alias("element_upper"),
    )
)

base_carts = (
    valid
    .filter(F.col("created_at") >= F.lit("2025-03-02 02:00:00").cast("timestamp"))
    .select(
        "created_at",
        "element",
        "element_filled_2",
        "created_at_hour",
        "resolved_element",
        "element_upper",
        F.date_trunc("month", "created_at").alias("created_at_period"),
        F.date_trunc("year", "created_at").alias("created_at_period_2"),
    )
)

result = (
    base_carts
    .groupBy(F.date_trunc("MONTH", "created_at_period").alias("activity_month"))
    .agg(
        F.round(F.avg("created_at_hour"), 2).alias("avg_created_at_hour"),
        F.percentile_approx("created_at_hour", [0.25, 0.5, 0.75]).alias("created_at_hour_quartiles"),
        F.min("created_at_period").alias("first_seen"),
    )
    .filter(F.col("avg_created_at_hour") >= 0)
    .orderBy(F.col("avg_created_at_hour").desc())
)
