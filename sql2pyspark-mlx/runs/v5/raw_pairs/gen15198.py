from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("units") == 20)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .select("id", "store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("store_id").isin([2, 3, 4, 5]))
        .select("id", "store_id")
    )
)
