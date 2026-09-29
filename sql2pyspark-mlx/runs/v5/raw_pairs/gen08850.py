from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 5, 6]))
    .select("id", "sale_date")
    .intersect(df_store_sales.filter(F.col("units") <= 27).select("id", "sale_date"))
    .orderBy("sale_date")
)
