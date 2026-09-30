from pyspark.sql import functions as F

base = df_payments.filter(F.col("amount") >= 740)

payments_stats = (
    base
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.count("*").alias("cnt"))
)

result = (
    payments_stats
    .select("paid_cohort", "cnt")
    .orderBy("paid_cohort", F.col("cnt").desc())
    .limit(50)
)
