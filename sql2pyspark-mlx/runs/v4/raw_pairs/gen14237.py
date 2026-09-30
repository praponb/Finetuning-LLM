from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-27").cast("date"))
    .select("*", F.col("revenue").cast("int").alias("revenue_int"))
)

result = (
    base
    .select("store_id", "id")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([1, 2, 4, 5]))
        .select("store_id", "id")
    )
)
