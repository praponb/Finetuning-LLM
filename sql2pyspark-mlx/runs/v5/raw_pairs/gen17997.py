from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("plan")

remaining_keys = (
    df_subscriptions
    .select("plan", "status")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee").between(200, 210))
        .select("plan", "status")
    )
)

reconciled = (
    df_subscriptions
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .withColumn("monthly_fee_percent_rank", F.percent_rank().over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
)

kept_keys = (
    enriched
    .select("status", "plan")
    .exceptAll(
        df_subscriptions
        .filter(F.col("status") == "EXPIRED")
        .select("status", "plan")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched_subscriptions = (
    verified
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.plan",
        "src.monthly_fee",
        "src.status",
        "src.monthly_fee_percent_rank",
        F.col("p.name"),
    )
)

with_dims = (
    enriched_subscriptions
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.signup_date").between("2023-06-01", "2024-03-01"))
    .select(
        "s.id",
        "s.customer_id",
        "s.monthly_fee",
        "s.monthly_fee_percent_rank",
        F.col("b.age"),
        F.col("b.city"),
    )
)

result = (
    with_dims
    .select("id", "customer_id", "monthly_fee", "monthly_fee_percent_rank", "age")
)
