from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w5 = Window.partitionBy("city")

result = (
    df_stores
    .select(
        "id",
        "store_name",
        F.dense_rank().over(w).alias("sqft_dense_rank"),
        F.avg("sqft").over(w2).alias("region_avg_sqft"),
        F.sum("sqft").over(w3).alias("running_sqft"),
        F.ntile(3).over(w4).alias("sqft_bucket"),
        F.min("sqft").over(w5).alias("city_min_sqft"),
    )
)
