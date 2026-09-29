from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("stock_qty").alias("min_product_stock_qty"))

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.launch_date")), "left_semi")
)

result = (
    not_flagged
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("brand").rlike("^(Initech|Umbrella)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Acme|Globex)$"), "GROUP_B").otherwise("CORE").alias("brand_zone"),
        F.col("min_product_stock_qty"),
    )
    .orderBy(
        F.col("min_product_stock_qty").desc_nulls_first(),
        F.col("brand_zone").desc(),
    )
)
