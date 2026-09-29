from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-04-17", "2025-02-01"))
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(
        df_store_sales
        .filter(F.month("sale_date").isin([2, 9, 12]))
        .select("sale_date", F.lit("store_sale").alias("source"))
    )
    .orderBy("shipped_date")
)
