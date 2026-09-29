from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "shipped_date").alias("shipped_month"))
    .agg(
        F.count("*").alias("shipment_count"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
    .orderBy(F.col("unique_carriers").desc(), F.col("shipment_count").desc())
)
