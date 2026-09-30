from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([6, 9, 12]))
    .select("status")
    .unionByName(df_shipments.filter(F.col("status") == "RETURNED").select("status"))
)
