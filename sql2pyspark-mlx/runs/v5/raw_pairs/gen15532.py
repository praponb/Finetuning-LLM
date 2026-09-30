from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2023-12-01")
    .select(
        "status",
        "order_id",
        F.dayofmonth("shipped_date").alias("shipped_date_day"),
    )
)
