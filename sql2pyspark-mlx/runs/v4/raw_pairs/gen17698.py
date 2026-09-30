from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("region", "city")

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-04-01")
    .select(
        "id",
        "city",
        "sqft",
        "region",
        F.rank().over(w).alias("rnk"),
        F.min("sqft").over(w2).alias("region_min_sqft"),
    )
)
