from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("status") == "IN_TRANSIT") & F.col("order_id").isin([9, 25, 39, 40])) | (F.col("cost") >= 50))
    .select("id", "cost")
)
