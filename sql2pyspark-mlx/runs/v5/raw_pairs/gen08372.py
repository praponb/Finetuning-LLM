from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(-6, -1)

metrics = (
    df_stores
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_2_prior"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "sqft_avg_2_prior",
        "sqft_avg_6_prior",
    )
    .orderBy(F.col("id").desc_nulls_last(), "sqft_avg_6_prior")
    .limit(5)
)
