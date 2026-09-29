from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit_plus_age").desc_nulls_first())

remaining_keys = (
    df_customers
    .select("id", "country")
    .intersect(
        df_customers
        .filter(F.upper("email").like("%ACK%"))
        .select("id", "country")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

kept = (
    kept_rows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") <= F.col("s.signup_date")), "left_anti")
)

base = (
    kept
    .filter(F.col("segment").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
    )
)

metrics = (
    base
    .filter(F.col("city").isin(["London", "Berlin", "Tokyo"]))
    .select(
        "name",
        "segment",
        "is_active",
        F.dense_rank().over(w).alias("credit_limit_plus_age_dense_rank"),
    )
)

stats = (
    metrics
    .groupBy("segment")
    .agg(
        F.countDistinct("segment").alias("distinct_segments"),
        F.sum(F.when(F.col("segment") == "SMB", F.col("credit_limit_plus_age_dense_rank")).otherwise(0)).alias("smb_credit_limit_plus_age_dense_rank"),
        F.round(F.sum("credit_limit_plus_age_dense_rank") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("credit_limit_plus_age_dense_rank_per_smb"),
        F.countDistinct("segment").alias("distinct_segments_2"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    stats
    .select(
        "segment",
        "distinct_segments",
        "smb_credit_limit_plus_age_dense_rank",
        "credit_limit_plus_age_dense_rank_per_smb",
        "distinct_segments_2",
        "num_rows",
    )
)
