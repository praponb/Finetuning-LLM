from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(~(F.col("delivered_date").between("2023-06-01", "2025-09-08") & (F.col("carrier") == "Flash")))
    .groupBy(
        "status",
        "carrier",
        F.when(F.col("carrier").isin(["Kerry", "DHL"]), "priority").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.min("cost").alias("min_cost"),
        F.min("shipped_date").alias("first_shipped_date"),
    )
)
