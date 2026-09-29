from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & ((F.col("order_id") == 28) | F.month("delivered_date").isin([3, 7, 11])))
    .select("id", "cost", "carrier", "order_id")
)
