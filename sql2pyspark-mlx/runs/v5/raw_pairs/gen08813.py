from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("carrier", "order_id")
    .agg(
        F.count("cost").alias("cost_count"),
        F.avg("cost").alias("avg_cost"),
        F.sum(F.when(F.col("carrier") == "DHL", 1).otherwise(0)).alias("dhl_count"),
    )
    .orderBy(F.col("avg_cost").desc(), F.col("carrier").desc())
)
