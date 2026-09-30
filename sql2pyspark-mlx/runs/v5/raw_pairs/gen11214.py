from pyspark.sql import functions as F

recent_payments = df_payments.filter(F.col("amount").isNotNull())

status_summary = (
    recent_payments
    .groupBy(
        "status",
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.min("paid_at").alias("min_paid_at"),
    )
)

result = (
    status_summary
    .select("status", "paid_cohort", "method_values", "min_paid_at")
    .orderBy("status")
)
