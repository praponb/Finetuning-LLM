from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .filter(F.col("avg_cost") >= 42)
)
