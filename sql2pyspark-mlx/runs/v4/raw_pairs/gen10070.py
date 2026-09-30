from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2024-03-13"))) & (F.col("sale_date") >= "2023-07-01"))
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        F.dayofweek("sale_date").alias("sale_date_dayofweek"),
    )
)

joined = (
    valid
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"), F.col("p.launch_date"))
)

joined_store_sales = (
    joined
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.*",
        F.col("p.stock_qty"),
        F.col("p.product_name"),
        F.col("p.launch_date").alias("product_launch_date"),
    )
)

result = (
    joined_store_sales
    .select("id", "units", "revenue", "category", "product_launch_date")
)
