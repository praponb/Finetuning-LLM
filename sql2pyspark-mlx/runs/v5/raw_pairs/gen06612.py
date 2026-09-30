from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount") > 590) & (F.col("discount") < 0.13))
    .groupBy("channel", "status")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
    )
    .orderBy("mobile_amount")
)
