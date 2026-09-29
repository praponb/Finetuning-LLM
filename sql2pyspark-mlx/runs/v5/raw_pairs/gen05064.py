from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_last = (
    df_products
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.tags",
        F.col("c.quantity"),
    )
)

result = (
    with_last
    .groupBy("brand")
    .agg(
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("stock_qty_per_home"),
        F.count("*").alias("event_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.min("quantity").alias("min_quantity"),
        F.sum(F.when(F.col("category") == "Sports", F.col("stock_qty")).otherwise(0)).alias("sports_stock_qty"),
        F.round(F.sum("stock_qty") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("stock_qty_per_electronics"),
    )
    .orderBy("stock_qty_per_electronics")
)
