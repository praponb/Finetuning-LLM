from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("id")
    .subtract(
        df_customers
        .filter(F.col("name").isNotNull())
        .select("id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("credit_limit") >= 18000, "PREMIUM").otherwise("STANDARD").alias("credit_limit_tier"),
    )
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.max("age").alias("max_age"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .orderBy(F.col("credit_limit_tier").desc())
)
