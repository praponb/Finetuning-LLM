from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "launch_date", "stock_qty")
    .orderBy("stock_qty", "id")
    .limit(20)
)
