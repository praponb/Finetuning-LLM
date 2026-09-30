from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2024)
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        F.datediff(F.current_date(), "shipped_date").alias("days_since_shipped_date"),
    )
    .orderBy("delivered_date", "id")
    .limit(10)
)
