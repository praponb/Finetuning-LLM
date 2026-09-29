from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-07-08").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.least("revenue", "units").alias("least_revenue_units"),
        F.col("units").cast("double").alias("units_str"),
    )
)

result = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.revenue",
        F.col("d.product_name"),
        F.col("d.category"),
    )
)
