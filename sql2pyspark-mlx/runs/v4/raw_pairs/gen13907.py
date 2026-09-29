from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-05-06", "2023-10-01") & (F.col("cost") <= 92) & F.col("order_id").isin([7, 39]))
    .select("id", "carrier", "shipped_date", "cost")
    .orderBy("shipped_date")
)
