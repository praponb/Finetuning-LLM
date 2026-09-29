from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isin(["FAILED", "REFUNDED", "SUCCESS"]) & F.hour("paid_at").between(10, 17) & F.col("order_id").isin([36, 39]))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.count("*").alias("total_count"))
)
