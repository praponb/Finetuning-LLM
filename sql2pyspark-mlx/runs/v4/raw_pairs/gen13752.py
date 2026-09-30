from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_2_prior"),
        F.ntile(5).over(w2).alias("sqft_bucket"),
        F.sum("sqft").over(w3).alias("running_sqft"),
    )
)
