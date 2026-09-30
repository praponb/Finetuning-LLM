from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-05-01").cast("date")) & (F.col("stock_qty") == 305))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper("brand").alias("brand_upper"),
    )
)

result = (
    base
    .select("id", "category")
    .exceptAll(
        df_products
        .filter(~F.col("brand").isin(["Stark", "Acme"]))
        .select("id", "category")
    )
)
