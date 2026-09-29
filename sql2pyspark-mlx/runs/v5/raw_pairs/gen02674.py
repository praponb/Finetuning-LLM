from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 7, 10]) & (F.col("units") <= 11) & F.col("product_id").isin([5, 8, 9, 10]))
    .select(
        "id",
        "product_id",
        "units",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
    .orderBy(F.col("units_filled").desc())
)
