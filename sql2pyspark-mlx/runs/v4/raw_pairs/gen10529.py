from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("category").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("category").orderBy("product_price", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_store_sales
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.price"), F.col("b.category"), F.col("b.cost"))
)

joined_store_sales = (
    joined
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price").alias("product_price"), F.col("p.stock_qty"))
)

filtered = (
    joined_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-07").cast("date"))
    .withColumn("price_per_revenue", F.round((F.col("price") / F.col("revenue")), 2))
)

with_windows = (
    filtered
    .withColumn("revenue_avg_5", F.avg("revenue").over(w))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    with_windows
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w2)), 4))
    .withColumn("revenue_avg_5_avg_2", F.avg("revenue_avg_5").over(w4))
    .withColumn("cumulative_cost", F.sum("cost").over(w5))
)
