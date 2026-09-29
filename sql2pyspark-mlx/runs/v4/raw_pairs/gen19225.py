from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("country", "segment")
w3 = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_first())
w4 = Window.orderBy(F.col("age").desc_nulls_last())
w_ordered = Window.partitionBy("city", "country").orderBy(F.col("age_percent_rank").desc_nulls_first())
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("city").orderBy(F.col("credit_limit_share").asc_nulls_first())
w8 = Window.partitionBy("city", "segment")

base = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-08-02").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.concat("country", F.lit(" "), "name").alias("country_name"),
        F.concat("segment", F.lit(" "), "city").alias("segment_city"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

matched_keys = (
    base
    .select("segment")
    .intersectAll(
        df_customers
        .filter(F.col("segment") == "Consumer")
        .select("segment")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .select(
        "*",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.percent_rank().over(w3).alias("age_percent_rank"),
        F.max("age").over(w).alias("segment_max_age"),
        F.percent_rank().over(w4).alias("age_percent_rank_2"),
    )
)

metrics_customers = (
    metrics
    .withColumn("segment_count_credit_limit_share", F.count("credit_limit_share").over(w))
    .withColumn("age_percent_rank_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("country_count_age_percent_rank", F.count("age_percent_rank").over(w6))
)

metrics_customers_2 = (
    metrics_customers
    .withColumn("credit_limit_share_percent_rank", F.percent_rank().over(w7))
    .withColumn("city_min_segment_count_credit_limit_share", F.min("segment_count_credit_limit_share").over(w8))
)

result = (
    metrics_customers_2
    .groupBy("segment")
    .agg(
        F.percentile_approx("country_count_age_percent_rank", [0.25, 0.5, 0.75]).alias("country_count_age_percent_rank_quartiles"),
        F.round(F.sum("corr_credit_limit_age") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("corr_credit_limit_age_per_corporate"),
        F.sum("credit_limit_share_percent_rank").alias("total_credit_limit_share_percent_rank"),
        F.percentile_approx("city_min_segment_count_credit_limit_share", 0.5).alias("city_min_segment_count_credit_limit_share_p50"),
    )
)
