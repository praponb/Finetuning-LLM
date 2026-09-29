from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id")
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w3 = Window.orderBy(F.col("sqft").desc())
w4 = Window.orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "*",
        F.ntile(5).over(w).alias("sqft_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.percent_rank().over(w3).alias("sqft_percent_rank"),
        F.lag("sqft", 1).over(w4).alias("prev_sqft"),
    )
)
