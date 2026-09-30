from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.partitionBy("city", "country")
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country")
w5 = Window.orderBy(F.col("credit_limit").desc())
w6 = Window.orderBy(F.col("age").asc_nulls_last())
w7 = Window.partitionBy("country", "city").orderBy(F.col("age_share").desc_nulls_first())
w_ordered = Window.orderBy(F.col("corr_credit_limit_age").desc_nulls_last())
w9 = Window.partitionBy("country").orderBy("corr_credit_limit_age")
w_running_2 = Window.partitionBy("city", "segment").orderBy(F.col("age_share_2").desc_nulls_last())
w11 = Window.orderBy(F.col("age").desc_nulls_first())
w12 = Window.partitionBy("segment", "country").orderBy(F.col("city_count_age_share_2").asc_nulls_last())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2024-04-06").cast("date")) & F.col("segment").isNotNull() & F.lower("email").like("%mma%"))
    .select(
        "*",
        F.date_sub("signup_date", 21).alias("signup_date_minus_21d"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

matched_keys = (
    valid
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("country").isin(["US", "DE", "JP"]))
        .select("city")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .select(
        "*",
        F.sum("credit_limit").over(w).alias("segment_sum_credit_limit"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share_2"),
    )
)

enriched_customers = (
    enriched
    .withColumn("segment_sum_credit_limit_share", F.round((F.col("segment_sum_credit_limit") / F.sum("segment_sum_credit_limit").over(w4)), 4))
    .withColumn("credit_limit_rank", F.rank().over(w5))
    .withColumn("age_percent_rank", F.percent_rank().over(w6))
    .withColumn("age_share_percent_rank", F.percent_rank().over(w7))
)

scored = (
    enriched_customers
    .withColumn("corr_credit_limit_age_dense_rank", F.dense_rank().over(w_ordered))
    .withColumn("city_count_age_share_2", F.count("age_share_2").over(w3))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w))
    .withColumn("corr_credit_limit_age_rank", F.rank().over(w9))
    .withColumn("age_share_2_percent_rank", F.percent_rank().over(w_running_2))
)

metrics = (
    scored
    .withColumn("age_rank", F.rank().over(w11))
    .withColumn("city_count_age_share_2_rank", F.rank().over(w12))
    .withColumn("city_max_segment_sum_credit_limit_share", F.max("segment_sum_credit_limit_share").over(w3))
    .withColumn("country_avg_age_percent_rank", F.avg("age_percent_rank").over(w4))
)

cleaned = (
    metrics
    .filter((F.col("signup_date") >= F.lit("2023-08-21").cast("date")) & F.col("city").isNotNull())
    .select(
        "*",
        (F.col("city_count_age_share_2") - F.col("corr_age_credit_limit")).alias("city_count_age_share_2_minus_corr_age_credit_limit"),
    )
)

by_type = (
    cleaned
    .groupBy("country")
    .pivot("segment", ["SMB", "Consumer", "Corporate"])
    .agg(F.sum("corr_credit_limit_age_rank").alias("amount"))
    .select(
        "country",
        F.col("SMB").alias("smb"),
        F.col("Consumer").alias("consumer"),
        F.col("Corporate").alias("corporate"),
    )
)

result = (
    by_type
    .select("country", "smb", "corporate")
    .orderBy(F.col("country").desc())
)
