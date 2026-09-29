from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 2) | F.col("delivered_date").between("2024-05-01", "2024-11-02"))
    .groupBy(
        F.month("shipped_date").alias("shipped_month"),
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.max("shipped_date").alias("last_shipped_date"),
        F.sum(F.when(F.col("carrier") == "UPS", 1).otherwise(0)).alias("ups_count"),
    )
)
