from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") == 2025)
    .select("id", "order_id", "cost", "carrier")
)
