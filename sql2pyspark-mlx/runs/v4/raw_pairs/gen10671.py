from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5]) & (F.col("units") < 21) & F.col("store_id").isin([2, 3]))
    .select("revenue")
)
