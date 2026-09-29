from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("revenue").between(1700, 3800) & (F.col("units") >= 26) & F.col("sale_date").between("2023-11-01", "2024-10-01")))
    .select(
        "product_id",
        "store_id",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
    .orderBy(F.col("product_id").desc())
)
