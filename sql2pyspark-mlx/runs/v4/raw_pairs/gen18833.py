from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
        F.initcap(F.trim("status")).alias("status_title"),
    )
)
