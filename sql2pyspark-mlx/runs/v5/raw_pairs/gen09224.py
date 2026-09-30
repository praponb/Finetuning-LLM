from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 35)
    .select("sale_date")
    .subtract(df_inventory.select(F.col("last_restock").alias("sale_date")))
)
