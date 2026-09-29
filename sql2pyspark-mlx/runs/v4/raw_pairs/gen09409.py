from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(66, 96) & (F.col("order_id").isNull() | F.col("status").isin(["LOST", "DELIVERED"])))
)
