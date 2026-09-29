from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())
w_ordered = Window.partitionBy("plan").orderBy("start_date", "id")
w5 = Window.partitionBy("plan")
w6 = Window.partitionBy("status").orderBy("credit_limit_whole", "id").rowsBetween(-3, -1)

joined = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.country"), F.col("d.name"), F.col("d.credit_limit"))
)

latest = (
    joined
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

cleaned = (
    latest
    .filter((F.col("end_date") >= F.lit("2023-05-19").cast("date")) & F.col("country").isNotNull() & F.month("end_date").isin([8, 10, 11]))
    .select("*", F.floor("credit_limit").alias("credit_limit_whole"))
)

with_windows = (
    cleaned
    .select(
        "*",
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.rank().over(w3).alias("monthly_fee_rank"),
        F.row_number().over(w_ordered).alias("event_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share"),
        F.avg("monthly_fee").over(w6).alias("monthly_fee_avg_3_prior"),
    )
)

remaining_keys = (
    with_windows
    .select("status", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id") == 17)
        .select("status", "plan")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("status")
    .agg(
        F.round(F.avg("cumulative_monthly_fee"), 2).alias("avg_cumulative_monthly_fee"),
        F.round(F.sum("monthly_fee_share") / F.nullif(F.count(F.when(F.col("status") == "EXPIRED", 1)), F.lit(0)), 2).alias("monthly_fee_share_per_expired"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
