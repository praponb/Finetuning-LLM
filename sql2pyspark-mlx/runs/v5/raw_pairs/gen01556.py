from pyspark.sql import functions as F

cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("cost").alias("min_product_cost"))

not_flagged = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.product_id").isin([1, 9, 10])), "left_anti")
)

result = (
    not_flagged
    .join(cost_by_supplier_id, on="supplier_id", how="left")
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
        F.when(F.col("category").rlike("^(Books|Home)$"), "PRIORITY").otherwise("EXTENDED").alias("category_zone"),
        F.col("min_product_cost"),
    )
    .orderBy(F.col("category").asc_nulls_first())
)
