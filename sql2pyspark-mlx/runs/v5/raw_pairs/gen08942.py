from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2025-09-01")
    .select(
        "id",
        "shipped_date",
        "carrier",
        "order_id",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.ceil("cost").alias("cost_whole"),
    )
)
