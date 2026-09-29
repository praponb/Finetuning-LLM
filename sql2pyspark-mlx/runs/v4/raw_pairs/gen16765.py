from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-09-01", "2024-09-27"))
    .select("id", "units")
    .subtract(df_store_sales.filter(F.col("store_id") == 3).select("id", "units"))
    .orderBy("units")
)
