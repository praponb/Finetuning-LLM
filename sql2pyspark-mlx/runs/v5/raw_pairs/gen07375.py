from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-02").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.round("revenue", 2).alias("revenue_rounded"),
    )
)

result = (
    filtered
    .select("store_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id") == 3)
        .select("store_id", "id")
    )
)
