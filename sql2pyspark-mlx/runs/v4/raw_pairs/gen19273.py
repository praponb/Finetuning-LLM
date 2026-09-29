from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id")
w2 = Window.partitionBy("brand").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .withColumn("price_bucket", F.ntile(4).over(w))
    .withColumn("last_known_cost", F.last("cost", ignorenulls=True).over(w2))
    .withColumn("cumulative_cost", F.sum("cost").over(w3))
)
