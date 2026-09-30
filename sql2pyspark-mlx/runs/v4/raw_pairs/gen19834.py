from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty") == 345)
    .select("id", "supplier_id", "launch_date")
)
