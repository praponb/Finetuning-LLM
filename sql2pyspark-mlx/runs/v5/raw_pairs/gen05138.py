from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") > 3800) & (F.col("units") >= 40) & F.col("product_id").isin([3, 7, 9]))
    .select(
        "units",
        "product_id",
        "revenue",
        F.round((F.col("units") * 1.15), 2).alias("adjusted_units"),
    )
)
