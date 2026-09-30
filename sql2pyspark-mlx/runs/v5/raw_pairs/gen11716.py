from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-04-01", "2025-09-01"))
    .select("shipped_date")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2, 8]))
        .select("sale_date")
    )
    .orderBy(F.col("shipped_date").desc())
)
