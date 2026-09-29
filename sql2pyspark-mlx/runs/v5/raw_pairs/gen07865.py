from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("units").between(27, 40))

agg = (
    src
    .groupBy("store_id")
    .agg(F.min("units").alias("min_units"))
)

result = (
    agg
    .filter(F.col("min_units") < 36)
    .select("store_id", "min_units")
)
