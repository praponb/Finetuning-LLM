from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("units").between(27, 37))

summary = (
    base
    .groupBy(F.year("sale_date").alias("sale_year"))
    .agg(F.min("sale_date").alias("min_sale_date"))
)

result = (
    summary
    .select("sale_year", "min_sale_date")
    .orderBy(F.col("sale_year").desc())
)
