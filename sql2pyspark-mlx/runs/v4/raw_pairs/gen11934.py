from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status", "method")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("method"))).alias("method_values"),
        F.count(F.when(F.col("method") == "paypal", 1)).alias("paypal_count"),
    )
    .filter(F.col("paypal_count") >= 1)
    .orderBy("paypal_count")
)
