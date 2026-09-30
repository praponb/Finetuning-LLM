from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 82) | (F.col("delivered_date") > "2024-10-01"))
    .select(
        "id",
        "status",
        "order_id",
        F.date_sub("delivered_date", 22).alias("delivered_date_minus_22d"),
    )
    .orderBy("id")
)
