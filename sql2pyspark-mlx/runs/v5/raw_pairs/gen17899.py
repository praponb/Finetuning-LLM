from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "region",
        "sqft",
        "city",
        F.min("sqft").over(w).alias("region_min_sqft"),
        F.row_number().over(w2).alias("rn"),
    )
)
