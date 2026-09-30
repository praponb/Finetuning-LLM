from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date_period", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "status").orderBy(F.col("end_date").desc_nulls_first())
w4 = Window.partitionBy("customer_id").orderBy("end_date", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("is_new_session").desc(), "id")
w6 = Window.partitionBy("customer_id").orderBy(F.col("start_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w7 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "basic"]))
    .select("*", F.date_trunc("WEEK", "start_date").alias("start_date_period"))
)

flagged = (
    valid
    .withColumn("session_start_flag", F.when(F.lag("start_date_period").over(w).isNull() | (F.col("start_date_period") > F.lag("start_date_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("burst_id", F.sum("session_start_flag").over(w2))

latest = (
    islands
    .withColumn("rank_in_group", F.rank().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    latest
    .withColumn("is_new_session", F.when(F.lag("start_date_period").over(w).isNull() | (F.col("start_date_period") > F.lag("start_date_period").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_subscriptions = gaps.withColumn("burst_id_2", F.sum("is_new_session").over(w2))

with_windows = (
    sessions_subscriptions
    .filter(F.col("rank_in_group") != 1)
    .withColumn("next_is_new_session", F.lead("is_new_session", 2).over(w4))
    .withColumn("is_new_session_bucket", F.ntile(10).over(w5))
    .withColumn("monthly_fee_sum_30d", F.sum("monthly_fee").over(w6))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w7))
)

cleaned = (
    with_windows
    .filter(F.col("next_is_new_session") == 0)
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        "start_date_period",
        "session_start_flag",
        "burst_id",
        "rank_in_group",
        "is_new_session",
        "burst_id_2",
        "next_is_new_session",
        "running_monthly_fee",
        F.concat_ws(",", "status", "plan").alias("status_plan_key"),
    )
)

result = (
    cleaned
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.segment").alias("segment"), F.col("s.plan").alias("plan"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.burst_id")).alias("min_burst_id"),
    )
)
