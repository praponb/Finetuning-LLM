from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_stores
    .select(
        "*",
        F.last("region", ignorenulls=True).over(w).alias("last_known_region"),
        F.avg("sqft").over(w2).alias("sqft_avg_2"),
    )
)
