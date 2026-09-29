from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.orderBy("sqft")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        F.rank().over(w).alias("rnk"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
