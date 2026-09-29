from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status").orderBy("is_paused", "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("start_date", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("status", "customer_id").orderBy("is_paused", "id")
w5 = Window.partitionBy("status").orderBy("start_date", "id")
w6 = Window.partitionBy("customer_id", "status").orderBy(F.col("seq_no_times_monthly_fee").desc_nulls_last(), "id")

cleaned = (
    df_subscriptions
    .filter((F.col("end_date") > F.lit("2024-04-07").cast("date")) & F.col("customer_id").isNotNull() & F.col("status").isin(["EXPIRED", "ACTIVE"]))
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.when(F.col("status") == "PAUSED", 1).otherwise(0).alias("is_paused"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.row_number().over(w2).alias("seq_no"),
        F.avg("is_paused").over(w3).alias("is_paused_avg_6"),
        F.row_number().over(w4).alias("event_seq"),
        F.lag("monthly_fee", 1).over(w5).alias("prev_monthly_fee"),
    )
)

cleaned_subscriptions = (
    with_windows
    .filter((F.col("start_date") >= F.to_date(F.lit("2024-03-20"))) & F.col("customer_id").isNotNull() & (F.col("prev_monthly_fee") > 160))
    .select(
        "*",
        (F.col("seq_no") * F.col("monthly_fee")).alias("seq_no_times_monthly_fee"),
    )
)

deduped = (
    cleaned_subscriptions
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

by_type = (
    deduped
    .groupBy("customer_id")
    .pivot("status", ["PAUSED", "EXPIRED"])
    .agg(F.avg("monthly_fee_share").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "customer_id",
        F.col("PAUSED_amount").alias("paused_amount"),
        F.col("PAUSED_cnt").alias("paused_cnt"),
        F.col("EXPIRED_amount").alias("expired_amount"),
        F.col("EXPIRED_cnt").alias("expired_cnt"),
    )
)

result = (
    by_type
    .select("customer_id", "paused_amount", "paused_cnt", "expired_amount")
    .orderBy(F.col("customer_id").desc())
)
