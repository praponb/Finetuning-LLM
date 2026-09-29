from pyspark.sql import functions as F

min_units = df_store_sales.filter(F.col("sale_date") <= "2025-10-01").agg(F.min("units")).first()[0]

result = (
    df_store_sales
    .filter(F.col("units") < min_units)
    .select("id", "units")
    .orderBy(F.col("units").desc(), F.col("id").desc())
)
