from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "delivered_date",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
    )
)
