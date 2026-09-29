from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("country", "customer_id").orderBy("last4_avg_monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy("last4_sum_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("last4_sum_monthly_fee").desc_nulls_first())
w5 = Window.partitionBy("country").orderBy("prev_last4_min_monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("country").orderBy(F.col("end_date").desc(), "id")

joined = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("p"), F.col("e.customer_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.country"), F.col("p.credit_limit"))
)

with_windows = (
    joined
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "status",
        "country",
        F.max("monthly_fee").over(w_grp).alias("last4_max_monthly_fee"),
        F.sum("monthly_fee").over(w_grp).alias("last4_sum_monthly_fee"),
        F.avg("monthly_fee").over(w_grp).alias("last4_avg_monthly_fee"),
        F.min("monthly_fee").over(w_grp).alias("last4_min_monthly_fee"),
    )
)

enriched = (
    with_windows
    .filter(F.month("end_date").isin([3, 9, 12]))
    .withColumn("prev_last4_min_monthly_fee", F.lag("last4_min_monthly_fee").over(w_running))
    .withColumn("next_last4_min_monthly_fee", F.lead("last4_min_monthly_fee").over(w_running))
    .withColumn("next_last4_avg_monthly_fee", F.lead("last4_avg_monthly_fee").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

enriched_subscriptions = (
    enriched
    .withColumn("running_prev_last4_min_monthly_fee", F.sum("prev_last4_min_monthly_fee").over(w3))
    .withColumn("last4_sum_monthly_fee_rank", F.rank().over(w4))
    .withColumn("cumulative_last4_min_monthly_fee", F.sum("last4_min_monthly_fee").over(w5))
    .withColumn("row_seq", F.row_number().over(w6))
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "customer_id",
        "last4_avg_monthly_fee",
        "last4_sum_monthly_fee",
        "prev_last4_min_monthly_fee",
    )
    .unpivot(
        ids=["id", "customer_id"],
        values=["last4_avg_monthly_fee", "last4_sum_monthly_fee", "prev_last4_min_monthly_fee"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
