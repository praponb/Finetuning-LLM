from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("status") == "SUCCESS", F.col("amount")).otherwise(0)).alias("success_amount"),
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("method")).alias("method_list"),
    )
    .orderBy(F.col("order_id").desc())
)
