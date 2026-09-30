from pyspark.sql import functions as F

valid = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("shipped_date") >= "2025-05-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.upper("carrier").alias("carrier_upper"),
    )
)

result = (
    valid
    .select("order_id")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isNull())
        .select("order_id")
    )
)
