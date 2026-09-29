from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w2 = Window.orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "city",
        "sqft",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.ntile(4).over(w2).alias("sqft_bucket_2"),
    )
)
