from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "delivered_date",
        "shipped_date",
        "carrier",
        F.length("status").alias("status_length"),
    )
    .orderBy("shipped_date")
)
