from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("is_consumer", "id")
w2 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w_grp = Window.partitionBy("country").orderBy("age", "id")

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("segment").eqNullSafe(F.lit("Corporate"))))
    .select(
        "*",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
    )
)

with_windows = (
    base
    .filter(~F.col("is_active"))
    .select(
        "*",
        F.ntile(4).over(w_recent).alias("is_consumer_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)

filtered = (
    with_windows
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-05-31")))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "row_seq",
        "event_seq",
        F.coalesce("age", F.lit(0)).alias("age_filled"),
        (F.col("event_seq") * F.col("age")).alias("event_seq_times_age"),
    )
)

valid = (
    filtered
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-04-20"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "row_seq",
        "event_seq",
        "event_seq_times_age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)

matched_keys = (
    valid
    .select("id", "city")
    .intersect(
        df_customers
        .filter(F.col("age") > 43)
        .select("id", "city")
    )
)

verified = (
    valid
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "row_seq",
    )
    .orderBy(F.col("signup_date").asc_nulls_first())
)
