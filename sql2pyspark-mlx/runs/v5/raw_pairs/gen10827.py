from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy(F.col("age").asc_nulls_last())
w_ordered = Window.partitionBy("city").orderBy(F.col("age_cume_dist").desc_nulls_first())
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("country")
w5 = Window.orderBy("credit_limit_rounded")
w6 = Window.orderBy(F.col("country_min_age").desc())
w7 = Window.orderBy(F.col("credit_limit_rounded").desc_nulls_first())
w8 = Window.orderBy(F.col("segment_min_age").asc_nulls_last())

filtered = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-03-30").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.round("credit_limit", 1).alias("credit_limit_rounded"),
        F.month("signup_date").alias("signup_date_month"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

remaining_keys = (
    filtered
    .select("id", "country")
    .subtract(
        df_customers
        .filter(F.col("email").like("%@company.co"))
        .select("id", "country")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

metrics = kept_rows.withColumn("age_cume_dist", F.cume_dist().over(w))

with_windows = (
    metrics
    .select(
        "is_active",
        "age",
        "credit_limit_rounded",
        "signup_date_month",
        "city_clean",
        F.cume_dist().over(w_ordered).alias("age_cume_dist_cume_dist"),
        F.min("age").over(w3).alias("segment_min_age"),
        F.min("age").over(w4).alias("country_min_age"),
    )
)

scored = (
    with_windows
    .withColumn("credit_limit_rounded_dense_rank", F.dense_rank().over(w5))
    .withColumn("country_min_age_percent_rank", F.percent_rank().over(w6))
    .withColumn("credit_limit_rounded_rank", F.rank().over(w7))
    .withColumn("segment_min_age_rank", F.rank().over(w8))
)

result = (
    scored
    .select(
        "is_active",
        "age",
        "credit_limit_rounded",
        "signup_date_month",
        "city_clean",
        "age_cume_dist_cume_dist",
        "segment_min_age",
        "country_min_age",
        "credit_limit_rounded_dense_rank",
    )
    .orderBy(F.col("credit_limit_rounded_dense_rank").desc_nulls_last())
)
