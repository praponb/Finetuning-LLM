from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand", "category").orderBy("launch_date", "id")

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([1, 3, 30, 37])), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([6, 7, 10])), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.stock_qty",
        "s.launch_date",
        F.col("c.revenue"),
        F.col("c.units"),
    )
)

deduped = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("id", "category", "brand", "revenue", "units")
)

kept = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([5, 20])), "left_anti")
)

result = (
    kept
    .groupBy("brand")
    .pivot("category", ["Grocery", "Home"])
    .agg(F.avg("revenue").alias("amount"))
    .select("brand", F.col("Grocery").alias("grocery"), F.col("Home").alias("home"))
)
