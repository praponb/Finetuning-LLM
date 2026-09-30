from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft")

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "East", "South"]))
    .select("id", "sqft", F.dense_rank().over(w).alias("sqft_dense_rank"))
)
