from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.count("*").alias("shipment_count"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") > 47)
    .drop("max_cost")
    .orderBy(F.col("cost_range").desc(), F.col("shipment_count").desc())
)
