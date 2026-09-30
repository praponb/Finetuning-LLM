from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("age").asc_nulls_last())
w2 = Window.partitionBy("status")
w3 = Window.orderBy(F.col("age_percent_rank").asc_nulls_first())
w4 = Window.partitionBy("status", "city")

filtered = (
    df_subscriptions
    .filter(F.col("start_date") > F.to_date(F.lit("2023-07-12")))
    .select("*", F.date_sub("start_date", 47).alias("start_date_minus_47d"))
)

kept_keys = (
    filtered
    .select("status", "id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee") > 180)
        .select("status", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.credit_limit"), F.col("b.age"), F.col("b.segment"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_customers.alias("c"), F.col("src.customer_id") == F.col("c.id"), "inner")
    .select("src.*", F.col("c.city"))
)

scored = (
    with_dims
    .select(
        "*",
        F.percent_rank().over(w).alias("age_percent_rank"),
        F.max("monthly_fee").over(w2).alias("status_max_monthly_fee"),
    )
)

scored_subscriptions = (
    scored
    .withColumn("age_percent_rank_rank", F.rank().over(w3))
    .withColumn("status_count_credit_limit", F.count("credit_limit").over(w4))
)

with_dims_subscriptions = (
    scored_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.segment").alias("customer_segment"), F.col("c.name"))
)

result = (
    with_dims_subscriptions
    .select(
        "city",
        F.col("credit_limit").alias("CREDIT_LIMIT_VAL"),
        F.col("monthly_fee").alias("MONTHLY_FEE_VAL"),
        F.col("age_percent_rank").alias("AGE_PERCENT_RANK"),
    )
    .unpivot(
        ["city"],
        ["CREDIT_LIMIT_VAL", "MONTHLY_FEE_VAL", "AGE_PERCENT_RANK"],
        "metric",
        "value",
    )
)
