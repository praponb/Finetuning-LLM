from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(
        F.when(F.col("status").isin(["REFUNDED", "SUCCESS"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.max("amount").alias("max_amount"),
        F.sum("amount").alias("total_amount"),
    )
)
