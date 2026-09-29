from pyspark.sql import functions as F

src = df_payments.filter(F.col("amount") >= 660)

method_summary = (
    src
    .groupBy("method", "status")
    .agg(
        F.countDistinct("amount").alias("unique_orders"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
)

result = (
    method_summary
    .filter(F.col("unique_orders") >= 9)
    .select("method", "status", "unique_orders", "stddev_amount")
    .orderBy(F.col("stddev_amount").desc(), "status")
)
