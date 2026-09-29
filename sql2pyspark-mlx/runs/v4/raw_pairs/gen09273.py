from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6]) & F.col("store_id").isin([2, 3, 4, 5]) & F.col("units").between(17, 33))
    .select(
        "revenue",
        "sale_date",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
    )
    .orderBy("sale_date")
)
