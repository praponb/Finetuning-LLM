from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w3 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("start_date", "id")
w5 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").asc_nulls_first(), "id")
w6 = Window.partitionBy("status").orderBy("seq_no", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("status", "plan").orderBy("start_date", "id")
w_hist = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("segment").orderBy("monthly_fee_share", "id")
w10 = Window.orderBy(F.col("end_date").desc(), "id")
w11 = Window.partitionBy("plan").orderBy(F.col("event_seq_2").asc_nulls_last(), "id")

scored = (
    df_subscriptions
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
        F.sum("monthly_fee").over(w3).alias("running_monthly_fee"),
        F.row_number().over(w4).alias("seq_no"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

joined = (
    ranked
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select("s.*", F.col("p.name"), F.col("p.signup_date"), F.col("p.segment"))
)

metrics = (
    joined
    .select(
        "id",
        "plan",
        "monthly_fee_bucket",
        "rn",
        "name",
        F.avg("monthly_fee_bucket").over(w6).alias("monthly_fee_bucket_avg_6"),
        F.lead("monthly_fee_share", 2).over(w_recent).alias("next_monthly_fee_share"),
        F.sum("monthly_fee").over(w_hist).alias("cumulative_monthly_fee"),
        F.row_number().over(w9).alias("event_seq"),
        F.row_number().over(w10).alias("event_seq_2"),
    )
)

deduped = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w11))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "monthly_fee_bucket",
        "name",
        "cumulative_monthly_fee",
        "event_seq",
        "event_seq_2",
    )
)

result = deduped.select("id", "event_seq_2")
