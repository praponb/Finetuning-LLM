from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-08-13")))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat_ws("/", "brand", "product_name").alias("brand_product_name_key"),
    )
)

result = (
    base
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_anti")
)
