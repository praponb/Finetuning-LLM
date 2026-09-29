from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee_filled").desc())
w_hist = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_filled_filled").desc())

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull() & F.col("customer_id").isNotNull())
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
    .withColumn("start_date_label", F.date_format("start_date", "yyyy-MM-dd"))
)

kept_keys = (
    valid
    .select("plan", "customer_id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("plan", "customer_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("monthly_fee").between(70, 100))
    .select(
        "*",
        F.coalesce("monthly_fee_filled", F.lit(0)).alias("monthly_fee_filled_filled"),
    )
)

scored = (
    filtered
    .withColumn("monthly_fee_filled_dense_rank", F.dense_rank().over(w))
    .withColumn("monthly_fee_filled_filled_rank", F.rank().over(w_hist))
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "start_date_label",
        "monthly_fee_filled_dense_rank",
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
