from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 38) | (F.col("sale_date") <= "2024-02-10"))
    .select("revenue", "sale_date", F.col("revenue").cast("int").alias("revenue_int"))
)
