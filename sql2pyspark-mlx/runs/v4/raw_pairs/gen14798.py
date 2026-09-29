from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date_period", "id")
w2 = Window.partitionBy("status").orderBy("start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("prev_start_date_period", "id")
w4 = Window.partitionBy("plan").orderBy("prev_start_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-09-30").cast("date")) & F.col("customer_id").isNotNull() & (F.col("end_date") <= "2023-04-01"))
    .select("*", F.date_trunc("week", "start_date").alias("start_date_period"))
)

base = (
    valid
    .filter(F.col("customer_id").isNotNull() & (F.to_date("start_date_period") < "2025-03-01"))
    .select(
        "*",
        F.col("monthly_fee").cast("int").alias("monthly_fee_int"),
        F.dayofweek("end_date").alias("end_date_dayofweek"),
    )
)

prev_events = base.withColumn("prev_start_date_period", F.lag("start_date_period").over(w))

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_start_date_period").isNull() | (F.col("start_date_period") > F.col("prev_start_date_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("session_id", F.sum("is_new_session").over(w2))

with_dims = (
    islands
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.segment"))
)

with_gaps = (
    with_dims
    .withColumn("is_new_session_2", F.when(F.lag("prev_start_date_period").over(w3).isNull() | (F.col("prev_start_date_period") > F.lag("prev_start_date_period").over(w3) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands_subscriptions = with_gaps.withColumn("burst_id", F.sum("is_new_session_2").over(w4))

result = (
    islands_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "monthly_fee",
        "status",
        "start_date_period",
        "monthly_fee_int",
        "end_date_dayofweek",
        "prev_start_date_period",
        F.when(F.col("plan").rlike("^(basic|premium)$"), "PRIORITY").otherwise("GROUP_A").alias("plan_zone"),
    )
)
