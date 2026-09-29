from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-10-09")))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.when(F.col("revenue") >= 3600, "High").when(F.col("revenue") >= 900, "Medium").otherwise("Low").alias("revenue_level"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"))
)
