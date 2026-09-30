from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc_nulls_first())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.max("price").alias("max_product_price"))

scored = (
    df_products
    .withColumn("running_count_stock_qty", F.count("stock_qty").over(w))
    .withColumn("running_avg_stock_qty", F.avg("stock_qty").over(w))
    .withColumn("running_max_stock_qty", F.max("stock_qty").over(w))
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w2))
)

result = (
    scored
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("max_product_price"),
    )
    .orderBy(F.col("brand").desc_nulls_last())
)
