from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "shipped_date",
        F.date_trunc("YEAR", "delivered_date").alias("delivered_date_period"),
    )
)
