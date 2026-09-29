from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter((F.col("units") <= 38) & (F.year("sale_date") == 2023))

agg = (
    recent_store_sales
    .groupBy("store_id", "product_id")
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    agg
    .filter(F.col("revenue_range") > 57)
    .select("store_id", "product_id", "revenue_range", "total_units")
    .orderBy("total_units")
)
