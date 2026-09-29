from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        F.min("sqft").over(w).alias("region_min_sqft"),
        F.first("region", ignorenulls=True).over(w2).alias("first_known_region"),
    )
)
