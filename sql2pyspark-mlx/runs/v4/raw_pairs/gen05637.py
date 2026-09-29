from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost").between(29, 33))
    .select("shipped_date")
    .subtract(df_orders.filter(F.col("status") == "SHIPPED").select("order_date"))
)
