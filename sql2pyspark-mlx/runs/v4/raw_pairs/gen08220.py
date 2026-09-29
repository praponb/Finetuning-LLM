from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.year("delivered_date") == 2024) & F.col("cost").between(57, 83)) | (F.col("shipped_date") >= "2024-12-31"))
    .select("status", "order_id", F.substring("carrier", 1, 2).alias("carrier_prefix"))
    .orderBy("carrier_prefix")
)
