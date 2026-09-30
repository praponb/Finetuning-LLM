from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("amount").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") > 3)
    .drop("unique_orders")
)
