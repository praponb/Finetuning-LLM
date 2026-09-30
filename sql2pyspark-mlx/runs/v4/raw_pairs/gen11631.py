from pyspark.sql import functions as F

src = (
    df_shipments
    .filter((F.col("carrier") != "FedEx") & (F.col("delivered_date") > "2024-01-01") & F.col("delivered_date").between("2023-09-01", "2025-06-01"))
)

shipments_stats = (
    src
    .groupBy("status")
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = shipments_stats.filter(F.col("avg_cost") > 61)
