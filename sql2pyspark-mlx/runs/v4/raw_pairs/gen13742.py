from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2025-07-28")
    .select("id", "order_id", "carrier")
    .unionByName(
        df_shipments
        .filter(F.col("order_id").isin([2, 11, 29, 38]))
        .select("id", "order_id", "carrier")
    )
    .distinct()
)
