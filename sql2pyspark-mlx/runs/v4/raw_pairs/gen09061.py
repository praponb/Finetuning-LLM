from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2024-04-24").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.substring("product_name", 1, 4).alias("product_name_prefix"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

result = (
    cleaned
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w))
    .withColumn("cumulative_price", F.sum("price").over(w2))
)
