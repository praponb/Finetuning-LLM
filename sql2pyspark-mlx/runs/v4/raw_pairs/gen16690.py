from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(16, 29))
    .select("revenue")
    .union(df_orders.select("amount"))
    .distinct()
    .orderBy("revenue")
)
