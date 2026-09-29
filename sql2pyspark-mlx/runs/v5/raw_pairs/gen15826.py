from pyspark.sql import functions as F

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id") == 37), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
    )
    .orderBy(F.col("cost").desc())
)
