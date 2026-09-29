from pyspark.sql import functions as F

recent_shipments = (
    df_shipments
    .filter(F.col("carrier").isin(["Kerry", "FedEx", "DHL"]) | F.col("delivered_date").between("2023-08-01", "2025-02-01"))
)

shipments_stats = (
    recent_shipments
    .groupBy(
        F.year("delivered_date").alias("delivered_year"),
        F.when(F.col("status").isin(["DELIVERED", "IN_TRANSIT"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(F.min("delivered_date").alias("first_delivered_date"))
)

result = shipments_stats.select("delivered_year", "status_group")
