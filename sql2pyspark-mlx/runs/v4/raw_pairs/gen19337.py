from pyspark.sql import functions as F

price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Laptop Pro 14|Desk Lamp)$"), "GROUP_B").when(F.col("product_name").rlike("^(wireless mouse|USB-C Hub)$"), "CORE").otherwise("OTHER_ZONE").alias("product_name_zone"),
        F.col("max_product_price"),
    )
    .orderBy(F.col("stock_qty").desc(), "launch_date")
)
