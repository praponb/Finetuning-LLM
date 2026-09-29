from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc())

result = (
    df_stores
    .select("id", "sqft", F.rank().over(w).alias("rnk"))
    .orderBy("sqft", F.col("id").desc())
)
