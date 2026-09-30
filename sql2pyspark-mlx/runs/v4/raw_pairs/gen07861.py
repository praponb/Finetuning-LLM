from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]) & ((F.col("units") == 34) | F.col("sale_date").between("2023-04-19", "2025-07-01")))
    .select("id", "store_id")
)
