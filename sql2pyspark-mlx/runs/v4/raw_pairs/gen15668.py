from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.year("delivered_date") == 2023) & F.month("delivered_date").isin([6, 10, 12])) | F.col("status").isin(["IN_TRANSIT", "LOST", "RETURNED"]))
    .select(
        "id",
        "cost",
        "carrier",
        "order_id",
        F.weekofyear("shipped_date").alias("shipped_date_weekofyear"),
    )
)
