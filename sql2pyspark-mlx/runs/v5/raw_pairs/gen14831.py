from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([13, 35]) & (F.to_date("paid_at") >= "2024-04-01") & (F.col("amount") < 670))
    .groupBy(
        F.when(F.col("status").isin(["SUCCESS", "FAILED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.count(F.when(F.col("status") == "FAILED", 1)).alias("failed_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
    )
    .filter(F.col("failed_count") > 5)
    .orderBy("method_values")
)
