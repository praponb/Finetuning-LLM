from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "Kerry", "DHL"]) & (F.col("order_id").isNull() | F.col("cost").between(29, 94)))
    .groupBy("status")
    .agg(F.min("shipped_date").alias("first_shipped_date"))
)
