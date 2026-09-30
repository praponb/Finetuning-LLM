from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") >= 4)
    .orderBy(F.col("unique_orders").desc(), F.col("status").desc())
)
