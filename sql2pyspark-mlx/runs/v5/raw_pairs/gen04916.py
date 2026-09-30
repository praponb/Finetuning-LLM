from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.avg("cost").alias("avg_cost"),
    )
    .filter(F.col("unique_carriers") < 5)
    .select("order_id", "unique_carriers", "avg_cost")
    .orderBy("order_id")
)
