from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("sqft")

result = (
    df_stores
    .filter(F.col("sqft") != 5336)
    .select("id", "sqft", F.dense_rank().over(w).alias("dense_rnk"))
)
