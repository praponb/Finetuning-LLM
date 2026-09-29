from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

result = (
    df_products
    .withColumn("max_cost_by_category", F.max("cost").over(w))
    .filter(F.col("cost") == F.col("max_cost_by_category"))
    .select("id", "category", "cost", "price")
    .orderBy(F.col("id").desc())
)
