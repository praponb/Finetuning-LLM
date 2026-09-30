from pyspark.sql import functions as F

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2024-01-18").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("is_active_flag") >= 64, "PREMIUM").otherwise("STANDARD").alias("is_active_flag_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_rows"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("us_events") > 1)
    .orderBy(F.col("us_rows").asc_nulls_first())
)
