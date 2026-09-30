from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id") == 26)
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("method")).alias("method_list"),
        F.sum(F.when(F.col("method") == "promptpay", 1).otherwise(0)).alias("promptpay_count"),
    )
    .filter(F.col("promptpay_count") >= 50)
    .select("method", "method_list", "promptpay_count")
    .orderBy("method")
)
