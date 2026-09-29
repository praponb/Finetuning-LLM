from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(14, 36))
    .groupBy("product_id")
    .agg(
        F.max("revenue").alias("max_revenue"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("max_revenue") < 3400)
    .orderBy(F.col("max_revenue").desc(), "product_id")
    .limit(5)
)
