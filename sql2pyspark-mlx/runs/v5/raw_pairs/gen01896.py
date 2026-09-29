from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "region").orderBy("sqft", "id")
w4 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region")

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        F.first("sqft", ignorenulls=True).over(w).alias("first_known_sqft"),
        F.avg("sqft").over(w2).alias("city_avg_sqft"),
        F.ntile(3).over(w3).alias("sqft_bucket"),
        F.last("sqft", ignorenulls=True).over(w4).alias("last_known_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w5)), 4).alias("sqft_share"),
    )
)
