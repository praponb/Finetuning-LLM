from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNull() & F.col("units").between(18, 33))
    .select(
        "id",
        "store_id",
        "product_id",
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)
