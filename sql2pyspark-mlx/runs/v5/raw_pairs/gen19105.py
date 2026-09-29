from pyspark.sql import functions as F

cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

with_dims = (
    df_products
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.supplier_name"))
)

result = (
    with_dims
    .join(cost_by_supplier_id, on="supplier_id", how="left")
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
        F.col("avg_product_cost"),
    )
)
