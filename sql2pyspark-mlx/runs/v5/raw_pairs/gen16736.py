from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([2, 16, 36, 38]))
    .select("id", "status", "order_id")
    .unionByName(
        df_shipments
        .filter(F.col("cost") > 66)
        .select("id", "status", "order_id")
    )
    .distinct()
)
