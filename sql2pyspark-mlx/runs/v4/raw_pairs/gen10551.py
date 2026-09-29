from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w_part = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w3 = Window.orderBy(F.col("corr_credit_limit_age").asc_nulls_first())

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2024-03-10").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.substring("city", 1, 2).alias("city_prefix"),
        F.date_add("signup_date", 43).alias("signup_date_plus_43d"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

kept_keys = (
    cleaned
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.col("credit_limit") >= 6700)
        .select("id", "segment")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "*",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.cume_dist().over(w_part).alias("credit_limit_cume_dist"),
        F.avg("age").over(w).alias("segment_avg_age"),
    )
)

result = (
    scored
    .select(
        "*",
        F.rank().over(w3).alias("corr_credit_limit_age_rank"),
        F.avg("credit_limit_cume_dist").over(w).alias("segment_avg_credit_limit_cume_dist"),
    )
)
