from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("order_id").isin([6, 10, 29, 33]))
    .select("amount")
    .intersect(df_shipments.filter(F.year("shipped_date") >= 2023).select("cost"))
)
