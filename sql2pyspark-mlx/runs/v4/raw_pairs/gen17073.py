from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id").isin([1, 3, 4, 6])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("category").rlike("^(Home|Fashion)$"), "GROUP_B").when(F.col("category").rlike("^(Electronics|Sports)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy("brand")
)
