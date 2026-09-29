from pyspark.sql import functions as F

result = (
    df_shipments
    .select(
        "id",
        "status",
        "carrier",
        "order_id",
        F.trim("status").alias("clean_status"),
    )
)
