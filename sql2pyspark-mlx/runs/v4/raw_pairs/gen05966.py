from pyspark.sql import functions as F

result = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_anti")
)
