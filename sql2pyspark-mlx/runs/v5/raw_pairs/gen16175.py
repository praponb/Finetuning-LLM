from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-02-17"))) & F.col("store_id").isin([1, 2]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .select("id", "product_id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([2, 10]))
        .select("id", "product_id")
    )
)
