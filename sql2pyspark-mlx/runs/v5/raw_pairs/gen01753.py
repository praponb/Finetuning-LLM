from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "UPS"]) | F.col("delivered_date").between("2023-12-01", "2024-06-01"))
    .select(
        "id",
        "order_id",
        "delivered_date",
        "shipped_date",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
    )
    .orderBy("days_since_delivered_date", "id")
)
