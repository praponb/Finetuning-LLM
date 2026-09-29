from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & (F.year("sale_date") == 2023) & F.col("units").between(22, 32))
    .select("revenue")
)
