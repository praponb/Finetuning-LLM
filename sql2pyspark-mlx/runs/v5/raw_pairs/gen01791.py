from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        F.concat("category", F.lit(" - "), "product_name").alias("category_product_name"),
        (F.col("stock_qty") + F.col("cost")).alias("stock_qty_plus_cost"),
    )
)

result = (
    base
    .groupBy("supplier_id")
    .pivot("brand", ["Initech", "Acme"])
    .agg(F.avg("stock_qty_plus_cost").alias("amount"))
    .select(
        "supplier_id",
        F.col("Initech").alias("initech"),
        F.col("Acme").alias("acme"),
    )
)
