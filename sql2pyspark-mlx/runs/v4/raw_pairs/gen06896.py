from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_first())

ranked = (
    df_subscriptions
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        "dedup_rank",
        F.when(F.col("status").rlike("^(PAUSED|ACTIVE)$"), "OTHER_ZONE").when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "GROUP_A").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy("status")
)
