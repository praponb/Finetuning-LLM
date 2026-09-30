from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id")

with_dims = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.name"))
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

kept_keys = (
    latest
    .select("id", "plan")
    .intersectAll(
        df_subscriptions
        .filter(F.col("plan") == "basic")
        .select("id", "plan")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(F.date_trunc("MONTH", "end_date").alias("activity_month"))
    .agg(
        F.percentile_approx("monthly_fee", [0.25, 0.5, 0.75]).alias("monthly_fee_quartiles"),
        F.avg("monthly_fee").alias("avg_monthly_fee"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.percentile_approx("monthly_fee", 0.95).alias("monthly_fee_p95"),
    )
    .orderBy(F.col("avg_monthly_fee").desc())
)
