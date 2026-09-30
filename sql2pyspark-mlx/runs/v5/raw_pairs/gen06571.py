from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.col("supplier_id") == 28)
    .select("id", "cost", F.dense_rank().over(w).alias("cost_dense_rank"))
    .orderBy(F.col("cost_dense_rank").desc())
)
