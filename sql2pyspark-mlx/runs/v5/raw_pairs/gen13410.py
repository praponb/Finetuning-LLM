from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").isNotNull() & F.col("units").between(14, 15) & F.col("product_id").isin([1, 3, 5, 7]))
)
