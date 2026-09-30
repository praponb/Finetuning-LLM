from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date_period", "id", "subscription_id")
w2 = Window.partitionBy("plan").orderBy("end_date_period", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "subscription_plan").orderBy(F.col("end_date_period").desc(), "id", "subscription_id")
w_ordered = Window.partitionBy("subscription_plan").orderBy("subscription_id", "id", "subscription_id")
w_grp = Window.partitionBy("subscription_plan").orderBy("monthly_fee", "id", "subscription_id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_subscriptions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        F.date_trunc("year", "end_date").alias("end_date_period"),
    )
)

with_window = (
    filtered
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & F.col("a.end_date_period").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee"),
    )
)

with_dims = (
    with_window
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .filter(F.length(F.col("p.name")) < 11)
    .select("e.*", F.col("p.city"), F.col("p.name"), F.col("p.email"))
)

with_prev = with_dims.withColumn("prev_end_date_period", F.lag("end_date_period").over(w))

gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_end_date_period").isNull() | (F.col("end_date_period") > F.col("prev_end_date_period") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_subscriptions = (
    gaps
    .select(
        "id",
        "plan",
        "end_date_period",
        "subscription_id",
        "subscription_plan",
        "monthly_fee",
        "name",
        "email",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

latest = (
    sessions_subscriptions
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "subscription_id", "subscription_plan", "monthly_fee", "burst_id")
)

with_windows = (
    latest
    .select(
        "*",
        F.lead("burst_id", 2).over(w_ordered).alias("next_burst_id"),
        F.avg("monthly_fee").over(w_grp).alias("monthly_fee_avg_3"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "subscription_id",
        "subscription_plan",
        "burst_id",
        "monthly_fee_avg_3",
        F.when(F.col("subscription_plan").rlike("^(premium|enterprise)$"), "GROUP_A").when(F.col("subscription_plan").rlike("^(basic|standard)$"), "CORE").otherwise("GROUP_B").alias("subscription_plan_zone"),
    )
)
