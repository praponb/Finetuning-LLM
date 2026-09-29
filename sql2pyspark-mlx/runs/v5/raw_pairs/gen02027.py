from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["web", "store"]), "core").otherwise("other").alias("channel_group"),
        F.quarter("order_date").alias("order_quarter"),
    )
    .agg(F.min("amount").alias("min_amount"))
    .filter(F.col("min_amount") >= 450)
    .select("channel_group", "order_quarter", "min_amount")
)
