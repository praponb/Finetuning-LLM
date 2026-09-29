from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("country")
    .exceptAll(
        df_customers
        .filter(F.col("age") == 58)
        .select("country")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
        "city",
        F.when(F.col("age") >= 63, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
    )
)
