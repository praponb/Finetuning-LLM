from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_first())

result = (
    df_products
    .select(
        "*",
        F.last("price", ignorenulls=True).over(w).alias("last_known_price"),
        F.ntile(10).over(w2).alias("cost_bucket"),
        F.dense_rank().over(w3).alias("price_dense_rank"),
    )
)
