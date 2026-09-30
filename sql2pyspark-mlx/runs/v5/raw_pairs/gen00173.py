from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("brand").isNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.size("tags").alias("tags_count"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

result = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") >= "2024-09-01"), "left_anti")
)
