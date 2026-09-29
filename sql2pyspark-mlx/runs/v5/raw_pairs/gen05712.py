from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("revenue")
    .subtract(df_orders.select("amount"))
)
