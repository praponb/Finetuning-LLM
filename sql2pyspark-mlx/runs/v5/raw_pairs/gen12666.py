from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_stores
    .filter(F.col("region") == "Central")
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.sum("sqft").over(w).alias("last7_sum_sqft"),
        F.min("sqft").over(w).alias("last7_min_sqft"),
        F.max("sqft").over(w).alias("last7_max_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)
