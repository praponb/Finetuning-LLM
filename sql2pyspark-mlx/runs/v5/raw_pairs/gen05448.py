from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_amount") > 7600)
    .drop("total_amount")
    .orderBy("status_values", "paid_cohort")
    .limit(1)
)
