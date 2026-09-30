from pyspark.sql import functions as F

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-04-23").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("is_active_flag") >= 70, "HIGH").otherwise("LOW").alias("is_active_flag_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.round(F.avg("is_active_flag"), 2).alias("avg_is_active_flag"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.sum(F.when(F.col("country") == "DE", F.col("is_active_flag")).otherwise(0)).alias("de_is_active_flag"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
    )
    .orderBy(F.col("credit_limit_p90").desc())
)
