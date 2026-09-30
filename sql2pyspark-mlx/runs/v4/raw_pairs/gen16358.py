from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("status")
w4 = Window.orderBy(F.col("is_active_flag").desc())
w5 = Window.orderBy(F.col("is_active_flag").asc_nulls_last())
w6 = Window.partitionBy("country")
w7 = Window.partitionBy("city", "subscription_plan")
w8 = Window.orderBy("is_active_flag_percent_rank")
w9 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_last())

with_latest = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.plan").alias("subscription_plan"))
)

ranked = (
    with_latest
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

matched_keys = (
    ranked
    .select("city", "segment")
    .subtract(
        df_customers
        .filter(F.month("signup_date").isin([1, 2, 11]))
        .select("city", "segment")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

valid = (
    reconciled
    .filter(F.col("status").isin(["ACTIVE", "CANCELLED"]))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("resolved_segment", F.coalesce("segment", F.lit("OTHER")))
)

base = (
    valid
    .filter(F.col("signup_date") >= F.to_date(F.lit("2024-04-10")))
    .select(
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "status",
        "subscription_plan",
        "rank_in_group",
        "is_active_flag",
        "resolved_segment",
        F.round((F.col("rank_in_group") * 1.15), 2).alias("adjusted_rank_in_group"),
    )
)

filtered = (
    base
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-04-27"))) & F.col("subscription_plan").isNotNull() & (F.col("is_active_flag") != 36))
    .select(
        "*",
        F.when(F.col("subscription_plan").isin(["basic", "standard"]), "Group A").otherwise("Other").alias("subscription_plan_group"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.sum("adjusted_rank_in_group").over(w3).alias("status_sum_adjusted_rank_in_group"),
        F.rank().over(w4).alias("is_active_flag_rank"),
        F.percent_rank().over(w5).alias("is_active_flag_percent_rank"),
    )
)

result = (
    scored
    .select(
        "*",
        F.min("is_active_flag_percent_rank").over(w6).alias("country_min_is_active_flag_percent_rank"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w7)), 4).alias("credit_limit_share"),
        F.dense_rank().over(w8).alias("is_active_flag_percent_rank_dense_rank"),
        F.cume_dist().over(w9).alias("credit_limit_cume_dist"),
    )
)
