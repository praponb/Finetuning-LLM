from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2024-08-01", "2024-09-17") & (F.year("delivered_date") >= 2025) & F.col("order_id").isin([9, 19, 20, 40]))
    .orderBy("shipped_date", F.col("cost").desc())
)
