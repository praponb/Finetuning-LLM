from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 30)
    .select(
        "id",
        "cost",
        "shipped_date",
        "delivered_date",
        F.date_trunc("month", "shipped_date").alias("shipped_date_period"),
    )
)
