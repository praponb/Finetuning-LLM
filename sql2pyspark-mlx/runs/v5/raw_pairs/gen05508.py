from pyspark.sql import functions as F

kept_keys = (
    df_customers
    .select("segment", "id")
    .subtract(
        df_customers
        .filter(F.col("signup_date") < "2023-03-01")
        .select("segment", "id")
    )
)

verified = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .rollup(F.when(F.col("age") >= 65, "LARGE").otherwise("SMALL").alias("age_tier"))
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles_2"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.count("*").alias("cnt"),
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.avg("credit_limit").alias("avg_credit_limit_2"),
    )
    .filter(F.col("avg_credit_limit_2") < 1000000)
)
