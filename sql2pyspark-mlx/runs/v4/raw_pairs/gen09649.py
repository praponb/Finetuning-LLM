from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2023-08-01")
    .select("shipped_date", "cost", "order_id")
)
