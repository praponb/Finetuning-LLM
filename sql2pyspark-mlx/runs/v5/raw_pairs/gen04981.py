from pyspark.sql import functions as F

lines = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("element"))

filtered = (
    lines
    .filter(F.col("element").isNotNull() & F.col("element").isin(["VIP", "BUNDLE", "SPRING10"]))
    .select(
        "*",
        F.initcap(F.trim("element")).alias("element_title"),
        F.date_format("created_at", "yyyy-MM-dd").alias("created_at_label"),
        F.coalesce("element", F.lit("N/A")).alias("resolved_element"),
    )
)

cleaned = (
    filtered
    .filter(F.col("element").isNotNull() & (F.col("created_at") >= "2023-06-12"))
    .withColumn("created_at_day", F.col("created_at").cast("date"))
    .withColumn("element_lower", F.lower("element"))
    .withColumn("resolved_element_2", F.coalesce("element", F.lit("N/A")))
)

base = (
    cleaned
    .filter(F.col("element") == "WELCOME5")
    .select(
        "id",
        "created_at",
        "element",
        "element_title",
        "resolved_element",
        "created_at_day",
        "element_lower",
        "resolved_element_2",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

result = (
    base
    .groupBy(F.date_trunc("DAY", "created_at").alias("activity_day"))
    .agg(
        F.round(F.sum("days_since_created_at") / F.nullif(F.count(F.when(F.col("element") == "WELCOME5", 1)), F.lit(0)), 2).alias("days_since_created_at_per_welcome5"),
        F.size(F.collect_set("element")).alias("num_elements"),
        F.min("created_at").alias("first_seen"),
    )
    .orderBy("days_since_created_at_per_welcome5")
)
