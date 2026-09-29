from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") != "promptpay") & (F.col("amount") >= 910))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy("paid_cohort")
)
