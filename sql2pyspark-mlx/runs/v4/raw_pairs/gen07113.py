from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("product_id").isin([6, 8, 10]) | F.col("product_id").isin([3, 4, 6, 7])))
    .select(
        "sale_date",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy(F.col("revenue").desc(), F.col("revenue_per_unit").desc())
)
