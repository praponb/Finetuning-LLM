from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.ntile(10).over(w2).alias("sqft_bucket"),
    )
)
