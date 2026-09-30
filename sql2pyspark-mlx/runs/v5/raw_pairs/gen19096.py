from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("cost").alias("sum_product_cost"))

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.stock_qty",
        "s.supplier_id",
        F.col("c.quantity"),
        F.col("c.last_restock"),
    )
)

result = (
    with_last
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select("id", "supplier_id", "quantity", F.col("sum_product_cost"))
)
