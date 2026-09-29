from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 2) & F.month("delivered_date").isin([2, 3, 11]) & (F.col("delivered_date") > "2025-03-01"))
    .select(
        "id",
        "carrier",
        "status",
        F.substring("carrier", 1, 4).alias("carrier_prefix"),
    )
    .orderBy("status")
)
