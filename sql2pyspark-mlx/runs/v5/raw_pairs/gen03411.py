from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(18, 20))
    .rollup(F.year("sale_date").alias("sale_year"), "store_id")
    .agg(
        F.sum("units").alias("total_units"),
        F.max("sale_date").alias("last_sale_date"),
    )
)
