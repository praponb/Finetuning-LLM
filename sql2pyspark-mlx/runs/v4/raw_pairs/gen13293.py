from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status").orderBy(F.col("monthly_fee").asc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc_nulls_first())

matched_keys = (
    df_subscriptions
    .select("customer_id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee") > 180)
        .select("customer_id")
    )
)

verified = (
    df_subscriptions
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w_ordered))
    .withColumn("monthly_fee_rank", F.rank().over(w2))
)

with_dims = (
    with_windows
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.name"), F.col("d.credit_limit"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.email"), F.col("b.country"), F.col("b.segment"))
)

filtered = (
    enriched
    .filter(F.col("end_date") > F.to_date(F.lit("2023-07-19")))
    .select(
        "*",
        F.least("monthly_fee_percent_rank", "monthly_fee").alias("least_monthly_fee_percent_rank_monthly_fee"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

base = (
    filtered
    .filter(F.col("end_date") >= F.lit("2023-07-08").cast("date"))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "monthly_fee_percent_rank",
        "monthly_fee_rank",
        "name",
        "country",
        "segment",
        "least_monthly_fee_percent_rank_monthly_fee",
        "resolved_country",
        F.when(F.col("monthly_fee_percent_rank") >= 0.63, "High").when(F.col("monthly_fee_percent_rank") >= 0.32, "Medium").otherwise("Low").alias("monthly_fee_percent_rank_level"),
        F.concat("country", F.lit(" "), "email").alias("country_email"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

result = (
    base
    .select(
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "monthly_fee_percent_rank",
        "monthly_fee_rank",
        "name",
        "country",
        "segment",
        F.when(F.col("plan").rlike("^(premium|standard)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|enterprise)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("plan_zone"),
    )
    .orderBy(F.col("monthly_fee_percent_rank").desc_nulls_first())
)
