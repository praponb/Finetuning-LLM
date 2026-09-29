from pyspark.sql import functions as F

result = (
    df_orders
    .rollup("channel")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount"))).alias("mobile_total_amount"),
    )
    .orderBy(F.col("channel").desc_nulls_last())
)
