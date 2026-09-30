from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & (F.col("status") == "RETURNED") & F.month("delivered_date").isin([5, 6, 12]))
    .select("id", "status", "delivered_date", "cost")
    .orderBy(F.col("id").desc(), F.col("cost").desc())
    .limit(5)
)
