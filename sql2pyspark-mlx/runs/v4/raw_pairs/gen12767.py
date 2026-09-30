from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([1, 7, 20, 35]))
    .select("id", "shipped_date", "carrier", "cost")
    .orderBy("carrier")
)
