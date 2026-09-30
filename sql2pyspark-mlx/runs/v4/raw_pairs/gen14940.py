from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())

result = (
    df_stores
    .select(
        "*",
        F.first("region", ignorenulls=True).over(w_hist).alias("first_known_region"),
        F.last("sqft", ignorenulls=True).over(w_hist).alias("last_known_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_2_prior"),
        F.dense_rank().over(w3).alias("sqft_dense_rank"),
    )
)
