from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("sale_date", F.lit("store_sale").alias("source"))
    .union(
        df_shipments
        .filter(F.col("shipped_date").between("2024-12-01", "2025-08-01"))
        .select("delivered_date", F.lit("shipment").alias("source"))
    )
)
