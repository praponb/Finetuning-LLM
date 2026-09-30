from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city", "region").orderBy("sqft")

result = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.avg("sqft").over(w2).alias("sqft_avg_5_prior"),
        F.cume_dist().over(w3).alias("sqft_cume_dist"),
    )
)
