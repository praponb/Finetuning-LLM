from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy("is_consumer", "id").rowsBetween(-2, -1)
w_recent = Window.partitionBy("segment", "city").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("is_consumer_avg_2_prior").desc_nulls_last())
w6 = Window.orderBy(F.col("country_count_credit_limit").asc_nulls_first())
w7 = Window.orderBy(F.col("row_seq").desc_nulls_first())
w8 = Window.orderBy(F.col("country_count_credit_limit").desc())

base = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-04-05").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

with_windows = (
    base
    .filter(F.col("is_active"))
    .select(
        "id",
        "name",
        "email_slug",
        "resolved_segment",
        F.row_number().over(w).alias("row_seq"),
        F.count("credit_limit").over(w2).alias("country_count_credit_limit"),
        F.avg("is_consumer").over(w3).alias("is_consumer_avg_2_prior"),
        F.first("city", ignorenulls=True).over(w_recent).alias("first_known_city"),
    )
)

valid = (
    with_windows
    .filter(F.col("country_count_credit_limit") >= 5)
    .select("*", F.regexp_replace("name", " ", "-").alias("name_slug"))
)

matched_keys = (
    valid
    .select("id")
    .exceptAll(
        df_customers
        .filter(F.col("name").isNotNull())
        .select("id")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "name",
        "is_consumer_avg_2_prior",
        "first_known_city",
        F.dense_rank().over(w5).alias("is_consumer_avg_2_prior_dense_rank"),
        F.percent_rank().over(w6).alias("country_count_credit_limit_percent_rank"),
        F.rank().over(w7).alias("row_seq_rank"),
        F.rank().over(w8).alias("country_count_credit_limit_rank"),
    )
)

result = (
    scored
    .select(
        "is_consumer_avg_2_prior",
        "country_count_credit_limit_percent_rank",
        "row_seq_rank",
        "country_count_credit_limit_rank",
    )
    .orderBy(F.col("country_count_credit_limit_rank").desc_nulls_first())
)
