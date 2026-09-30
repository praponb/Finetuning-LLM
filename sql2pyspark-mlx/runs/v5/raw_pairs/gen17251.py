from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date").between("2024-03-26", "2024-11-18"))
    .select("id", "units", "revenue", "store_id", F.floor("units").alias("units_whole"))
    .orderBy("revenue")
)
