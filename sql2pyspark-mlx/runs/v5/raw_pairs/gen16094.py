from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("cost").desc())
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_whole").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("cost", "id")
w4 = Window.orderBy("cost", "id")
w5 = Window.partitionBy("product_id").orderBy("latest_rank", "id").rowsBetween(-5, -1)
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

valid = (
    df_inventory
    .filter(F.col("product_id").isin([2, 4, 5, 9]))
    .withColumn("quantity_whole", F.floor("quantity"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.product_id",
        "e.quantity_whole",
        F.col("d.cost"),
        F.col("d.stock_qty"),
        F.col("d.brand"),
    )
)

latest = (
    with_dims
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

scored = (
    deduped
    .withColumn("cost_bucket", F.ntile(4).over(w3))
    .withColumn("prev_stock_qty", F.lag("stock_qty", 1).over(w4))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w5))
)

summary = (
    scored
    .groupingSets([["brand", "store_id"], ["brand"]], "brand", "store_id")
    .agg(
        F.count(F.when(F.col("brand") == "Acme", 1)).alias("acme_rows"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.count("*").alias("num_rows"),
    )
)

result = (
    summary
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "brand",
        "store_id",
        "acme_rows",
        "num_brands",
        "num_rows",
        F.col("sum_inventory_quantity"),
    )
    .orderBy("sum_inventory_quantity")
)
