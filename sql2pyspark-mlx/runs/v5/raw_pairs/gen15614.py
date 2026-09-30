from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-03-20")
    .groupBy("method")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("amount").alias("unique_methods"),
        F.max("paid_at").alias("latest_paid_at"),
    )
    .orderBy(F.col("method").desc())
)
