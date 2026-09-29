from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy("sqft")
w4 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, -1)

result = (
    df_stores
    .select(
        "*",
        F.min("sqft").over(w).alias("region_min_sqft"),
        F.avg("sqft").over(w2).alias("city_avg_sqft"),
        F.rank().over(w3).alias("sqft_rank"),
        F.sum("sqft").over(w4).alias("running_sqft"),
        F.avg("sqft").over(w5).alias("sqft_avg_6_prior"),
    )
)
