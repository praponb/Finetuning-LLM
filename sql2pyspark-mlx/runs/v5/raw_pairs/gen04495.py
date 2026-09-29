from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") >= "2023-06-01") & (F.col("revenue").isNull() | (F.col("sale_date") < "2023-12-01")))
    .select("id", "store_id", "units")
)
