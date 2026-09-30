from pyspark.sql import functions as F

matched_keys = (
    df_customers
    .select("country", "id")
    .intersectAll(
        df_customers
        .filter(~F.col("city").isin(["Berlin", "Bangkok", "Singapore"]))
        .select("country", "id")
    )
)

reconciled = (
    df_customers
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("age") >= 63, "HIGH").otherwise("LOW").alias("age_tier"),
        F.date_trunc("week", "signup_date").alias("week_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("country") == "TH", F.col("credit_limit"))).alias("th_total_credit_limit"),
        F.avg(F.when(F.col("segment") == "Consumer", F.col("credit_limit"))).alias("consumer_avg_credit_limit"),
        F.count("*").alias("row_count"),
        F.count("*").alias("cnt"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
