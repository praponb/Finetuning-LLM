from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date")

ranked = (
    df_subscriptions
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter((F.col("end_date") >= F.to_date(F.lit("2023-09-04"))) & F.col("customer_id").isNotNull())
    .select("*", F.lower("plan").alias("plan_lower"))
)

kept_keys = (
    base
    .select("customer_id", "status")
    .subtract(
        df_subscriptions
        .filter(F.month("start_date").isin([1, 7, 8]))
        .select("customer_id", "status")
    )
)

reconciled = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("status", "id")
    .exceptAll(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("status", "id")
    )
)

reconciled_subscriptions = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    reconciled_subscriptions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select(
        "src.customer_id",
        "src.end_date",
        "src.monthly_fee",
        "src.status",
        "src.latest_rank",
        F.col("b.email"),
        F.col("b.country"),
        F.col("b.name"),
    )
)

joined_subscriptions = (
    joined
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.credit_limit"), F.col("p.age"))
)

result = (
    joined_subscriptions
    .select(
        "customer_id",
        "end_date",
        "latest_rank",
        "email",
        "country",
        "credit_limit",
        F.when(F.col("country").rlike("^(JP|US)$"), "PRIORITY").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy("customer_id", "country_zone")
)
