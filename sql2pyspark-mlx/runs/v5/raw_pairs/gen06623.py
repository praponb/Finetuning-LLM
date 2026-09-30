from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy(F.col("sqft").asc_nulls_first())
w_running = Window.orderBy(F.col("sqft").desc_nulls_first())

metrics = (
    df_stores
    .filter(F.col("region").isin(["East", "West"]))
    .select(
        "id",
        "store_name",
        F.max("sqft").over(w).alias("city_max_sqft"),
        F.cume_dist().over(w2).alias("sqft_cume_dist"),
        F.cume_dist().over(w_running).alias("sqft_cume_dist_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "store_name",
        "sqft_cume_dist",
        "sqft_cume_dist_2",
        F.when(F.col("store_name").rlike("^(Siam Flagship|Downtown Express)$"), "EXTENDED").when(F.col("store_name").rlike("^(Airport Kiosk|Mall Central)$"), "CORE").otherwise("PRIORITY").alias("store_name_zone"),
    )
)
