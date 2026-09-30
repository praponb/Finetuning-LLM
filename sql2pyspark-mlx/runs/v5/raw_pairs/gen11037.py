from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w3 = Window.orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("status", "plan")
w5 = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc())
w6 = Window.partitionBy("status").orderBy(F.col("monthly_fee_bucket_3").asc_nulls_first(), "id")

cleaned = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.concat_ws("-", "status", "plan").alias("status_plan_key"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.ntile(5).over(w_grp).alias("monthly_fee_bucket"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket_2"),
        F.ntile(4).over(w3).alias("monthly_fee_bucket_3"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
        F.cume_dist().over(w5).alias("monthly_fee_cume_dist"),
    )
)

deduped = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    deduped
    .select("id", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("monthly_fee").isNotNull())
        .select("id", "status")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("MONTH", "start_date").alias("activity_month"))
    .agg(
        F.sum(F.when(F.col("status") == "EXPIRED", F.col("monthly_fee_bucket_3")).otherwise(0)).alias("expired_monthly_fee_bucket_3"),
        F.round(F.sum("monthly_fee_bucket_2") / F.nullif(F.count(F.when(F.col("status") == "PAUSED", 1)), F.lit(0)), 2).alias("monthly_fee_bucket_2_per_paused"),
        F.round(F.avg("monthly_fee_cume_dist"), 2).alias("avg_monthly_fee_cume_dist"),
    )
)
