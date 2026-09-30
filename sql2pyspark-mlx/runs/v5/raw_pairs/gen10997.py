from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.max("paid_at").alias("ended_at"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("status") == "SUCCESS", F.col("amount"))).alias("success_max_amount"),
        F.countDistinct("status").alias("distinct_status"),
    )
)
