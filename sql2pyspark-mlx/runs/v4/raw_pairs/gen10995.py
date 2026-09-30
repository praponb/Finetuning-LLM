from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 13)
    .select("id", "sale_date", "product_id")
)
