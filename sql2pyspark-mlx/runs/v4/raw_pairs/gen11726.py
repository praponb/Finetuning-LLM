from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("revenue") > 3100)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.greatest("units", "revenue").alias("greatest_units_revenue"),
        F.floor("revenue").alias("revenue_whole"),
    )
)

result = (
    cleaned
    .select("id", "store_id", "greatest_units_revenue", "revenue")
    .unpivot(
        ["id", "store_id"],
        ["greatest_units_revenue", "revenue"],
        "kpi",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
