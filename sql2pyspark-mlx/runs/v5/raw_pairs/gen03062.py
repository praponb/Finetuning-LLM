from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("amount").isNull() & F.col("order_id").isin([17, 29]))
    .withColumn("status_group", F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "Tier 1").otherwise("Other"))
)

summary = (
    src
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"))
)

result = summary.select("paid_cohort", "card_count")
