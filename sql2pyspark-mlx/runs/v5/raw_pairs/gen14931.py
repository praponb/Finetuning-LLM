from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["PENDING", "RETURNED"]))
    .groupBy("channel")
    .agg(
        F.countDistinct("amount").alias("unique_statuss"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
    )
    .filter(F.col("unique_statuss") < 4)
)
