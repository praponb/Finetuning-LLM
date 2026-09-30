from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-04-11"))) & F.col("revenue").between(1100, 2800))
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .select("id", "store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([2, 6]))
        .select("id", "store_id")
    )
)
