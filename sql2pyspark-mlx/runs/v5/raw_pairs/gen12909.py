from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(-6, -1)
w_ordered = Window.partitionBy("city")
w4 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
        F.avg("sqft").over(w_ordered).alias("city_avg_sqft"),
        F.avg("sqft").over(w4).alias("sqft_avg_5"),
        F.avg("sqft").over(w5).alias("sqft_avg_5_2"),
    )
)
