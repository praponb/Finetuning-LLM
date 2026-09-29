from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(((F.col("revenue") <= 3800) & F.col("store_id").isin([5, 6])) | F.col("revenue").between(3600, 4000))
    .select(
        "id",
        "store_id",
        "units",
        F.least("revenue", "units").alias("least_revenue_units"),
    )
    .orderBy(F.col("least_revenue_units").desc(), "id")
    .limit(10)
)
