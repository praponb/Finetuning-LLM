from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id")
w2 = Window.partitionBy("region")

result = (
    df_stores
    .filter(F.length("store_name") > 6)
    .select(
        "id",
        "region",
        "sqft",
        F.row_number().over(w).alias("rn"),
        F.sum("sqft").over(w2).alias("region_sum_sqft"),
    )
)
