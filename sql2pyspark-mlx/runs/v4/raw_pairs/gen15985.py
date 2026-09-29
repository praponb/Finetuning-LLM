from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("total_count"),
        F.avg("amount").alias("avg_amount"),
    )
    .filter(F.col("avg_amount") < 1430)
    .drop("avg_amount")
    .orderBy("total_count", "method")
    .limit(20)
)
