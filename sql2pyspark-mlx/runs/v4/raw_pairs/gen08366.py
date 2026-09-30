from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") <= 93)
    .select(
        "status",
        "order_id",
        "cost",
        F.weekofyear("delivered_date").alias("delivered_date_weekofyear"),
    )
)
