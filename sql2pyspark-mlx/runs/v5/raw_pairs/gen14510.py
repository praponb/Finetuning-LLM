from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 10)
    .select(
        "id",
        "sale_date",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue_per_unit").desc(), "sale_date")
)
