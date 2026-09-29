from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id", F.year("launch_date").alias("launch_year"))
    .agg((F.max("price") - F.min("price")).alias("price_range"))
    .filter(F.col("price_range") < 65)
    .select("supplier_id", "launch_year", "price_range")
    .orderBy(F.col("launch_year").desc(), "supplier_id")
    .limit(10)
)
