from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("carrier")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("avg_cost") > 54)
)
