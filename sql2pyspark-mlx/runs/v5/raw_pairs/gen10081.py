from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-01-25")
    .select("carrier", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isNotNull())
        .select("carrier", "shipped_date")
    )
    .distinct()
)
