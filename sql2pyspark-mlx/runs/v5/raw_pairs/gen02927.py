from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("start_date").desc_nulls_last(), "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("end_date").asc_nulls_last())
w3 = Window.partitionBy("plan", "status").orderBy(F.col("rank_in_group").desc(), "id")

valid = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-10-09")))
    .withColumn("is_active", F.when(F.col("status") == "ACTIVE", 1).otherwise(0))
    .withColumn("status_plan", F.concat("status", F.lit(", "), "plan"))
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
)

deduped_subscriptions = (
    latest
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

filtered = (
    deduped_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee",
        "status",
        "is_active",
        "status_plan",
        "rank_in_group",
        "latest_rank",
        "rn",
        (F.col("latest_rank") * F.col("rank_in_group")).alias("latest_rank_times_rank_in_group"),
    )
)

wide = (
    filtered
    .groupBy("status")
    .pivot("plan", ["enterprise", "premium"])
    .agg(F.sum("latest_rank_times_rank_in_group"))
)

result = (
    wide
    .select(
        "status",
        "enterprise",
        "premium",
        F.when(F.col("status").rlike("^(PAUSED|EXPIRED)$"), "OTHER_ZONE").when(F.col("status").rlike("^(ACTIVE|CANCELLED)$"), "GROUP_B").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("status_zone").asc_nulls_first())
)
