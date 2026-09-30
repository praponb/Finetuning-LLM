from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([1, 5]) & (F.col("status") != "RETURNED") & F.col("delivered_date").between("2023-10-01", "2024-06-01"))
    .select(
        "id",
        "order_id",
        F.month("shipped_date").alias("shipped_date_month"),
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
)
