from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") <= "2025-01-01") | (F.col("status") != "DELIVERED"))
    .select(
        "id",
        "order_id",
        F.date_format("shipped_date", "MMM yyyy").alias("shipped_date_label"),
    )
)
