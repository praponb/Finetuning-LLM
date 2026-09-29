from pyspark.sql import functions as F

stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("stock_qty").alias("avg_product_stock_qty"))

joined = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.country"))
)

result = (
    joined
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "country",
        F.when(F.col("brand").rlike("^(Umbrella|Acme)$"), "OTHER_ZONE").when(F.col("brand").rlike("^(Globex|Initech)$"), "PRIORITY").otherwise("CORE").alias("brand_zone"),
        F.col("avg_product_stock_qty"),
    )
    .orderBy("avg_product_stock_qty")
)
