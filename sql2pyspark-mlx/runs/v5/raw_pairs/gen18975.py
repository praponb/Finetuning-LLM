from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-02-09")
    .select(
        "cost",
        "delivered_date",
        "shipped_date",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
)
