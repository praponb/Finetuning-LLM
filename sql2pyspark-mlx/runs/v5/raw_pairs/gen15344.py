from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-02-15").cast("date")) & (F.col("revenue") > 2200))
    .select(
        "*",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .select("product_id", "id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue") < 3600)
        .select("product_id", "id")
    )
)
