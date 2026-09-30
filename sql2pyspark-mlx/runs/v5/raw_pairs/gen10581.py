from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(900, 1340) & F.col("status").isin(["SUCCESS", "REFUNDED", "FAILED"]) & (F.col("amount") < 1140))
    .groupBy(
        "method",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
    .orderBy("method")
)
