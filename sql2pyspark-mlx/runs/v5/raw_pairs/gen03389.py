from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity", "id")

deduped = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped_inventory = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "quantity", "last_restock", "row_num")
)

metrics = (
    deduped_inventory
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.ntile(3).over(w4).alias("quantity_bucket"),
    )
)

enriched = (
    metrics
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.size(F.col("p.tags")) >= 2)
    .select("a.*", F.col("p.price"))
)

result = (
    enriched
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("s.quantity_bucket")).alias("min_quantity_bucket"),
        F.sum(F.col("s.price")).alias("total_price"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
