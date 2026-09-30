from pyspark.sql import functions as F

max_units = df_store_sales.filter(F.col("store_id").isin([1, 2, 4, 6])).agg(F.max("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") >= max_units)
    .select("id", "units", "revenue", "store_id")
)
