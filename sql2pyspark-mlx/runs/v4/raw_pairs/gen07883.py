from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("order_id")
    .agg(
        F.min("amount").alias("min_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy("order_id", F.col("min_amount").desc())
)
