from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").asc_nulls_first())
w2 = Window.partitionBy("brand", "category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id", "category").orderBy(F.col("price").desc_nulls_first())
w4 = Window.partitionBy("category", "brand")

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("stock_qty_minus_price", (F.col("stock_qty") - F.col("price")))
)

result = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.percent_rank().over(w3).alias("price_percent_rank"),
        F.min("price").over(w4).alias("category_min_price"),
    )
)
