from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").desc(), "id")
w2 = Window.partitionBy("status")

metrics = (
    df_subscriptions
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
)

result = (
    metrics
    .withColumn("week_start", F.date_trunc("WEEK", "start_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "start_date"))
    .groupingSets(
        [["week_start", "quarter_start"], ["week_start"]],
        "week_start",
        "quarter_start",
    )
    .agg(
        F.round(F.avg("monthly_fee_share"), 2).alias("avg_monthly_fee_share"),
        F.percentile_approx("monthly_fee", 0.9).alias("monthly_fee_p90"),
        F.round(F.sum("monthly_fee_share") / F.nullif(F.count(F.when(F.col("status") == "EXPIRED", 1)), F.lit(0)), 2).alias("monthly_fee_share_per_expired"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("week_start").desc_nulls_first())
)
