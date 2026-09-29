from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w3 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc_nulls_last())
w5 = Window.orderBy("sqft", "id")

result = (
    df_stores
    .select(
        "*",
        F.sum("sqft").over(w).alias("region_sum_sqft"),
        F.percent_rank().over(w2).alias("sqft_percent_rank"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
        F.cume_dist().over(w4).alias("sqft_cume_dist"),
        F.ntile(5).over(w5).alias("sqft_bucket"),
    )
)
