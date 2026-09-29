from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(640, 740) | (F.col("method") == "cash"))
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        "status",
    )
    .agg(F.count("*").alias("num_payments"))
    .orderBy(F.col("num_payments").desc())
)
