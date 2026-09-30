from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") >= "2023-09-01")
    .groupBy(F.date_format("paid_at", "yyyy-MM").alias("paid_month"), "status")
    .agg(
        F.count(F.when(F.col("status") == "REFUNDED", 1)).alias("refunded_count"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("refunded_count").desc())
)
