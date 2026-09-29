from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc())
w5 = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")
w6 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())

with_windows = (
    df_customers
    .filter(F.col("city") != "Bangkok")
    .select(
        "*",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_3_prior"),
        F.avg("age").over(w2).alias("age_avg_2_prior"),
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
        F.percent_rank().over(w4).alias("credit_limit_percent_rank"),
        F.ntile(5).over(w5).alias("age_bucket"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.rank().over(w6))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    deduped
    .select("segment", "city")
    .subtract(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("segment", "city")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

valid = (
    kept_rows
    .filter(F.col("segment").isNotNull())
    .select(
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "age_avg_2_prior",
        "corr_age_credit_limit",
        "credit_limit_percent_rank",
        "age_bucket",
        "row_num",
        F.coalesce("credit_limit", F.lit(0)).alias("credit_limit_filled"),
    )
)

result = (
    valid
    .groupBy(
        F.when(F.col("age_avg_2_prior") >= 67, "PREMIUM").otherwise("STANDARD").alias("age_avg_2_prior_tier"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.countDistinct("country").alias("distinct_countries"),
    )
    .filter(F.col("distinct_cities") < 1000)
    .orderBy(F.col("distinct_cities").desc())
)
