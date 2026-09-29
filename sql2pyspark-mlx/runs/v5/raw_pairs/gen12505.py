from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("min_cost_by_category", F.min("cost").over(w))
    .filter(F.col("cost") == F.col("min_cost_by_category"))
    .select("id", "category", "cost", "stock_qty")
    .orderBy(F.col("cost").desc())
)
