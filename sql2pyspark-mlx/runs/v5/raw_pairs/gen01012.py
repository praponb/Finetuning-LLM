from pyspark.sql import functions as F

src = df_shipments.filter(F.col("delivered_date").between("2023-10-01", "2025-03-01"))

shipments_stats = (
    src
    .groupBy("carrier", "status")
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.count("*").alias("total_count"),
    )
)

result = shipments_stats.select("carrier", "status", "unique_carriers")
