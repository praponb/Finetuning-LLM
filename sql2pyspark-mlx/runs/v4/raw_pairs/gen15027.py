from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("cost")

result = (
    df_products
    .filter(F.array_contains("tags", "new"))
    .select("id", "brand", "cost", F.dense_rank().over(w).alias("cost_dense_rank"))
)
