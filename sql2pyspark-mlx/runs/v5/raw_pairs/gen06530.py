from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
        F.date_format("paid_at", "yyyy-MM").alias("paid_month"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("status") == "REFUNDED", F.col("amount")).otherwise(0)).alias("refunded_amount"),
    )
    .filter(F.col("refunded_amount") >= 33)
)
