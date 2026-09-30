from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc())
w2 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("store_id", "product_id")

with_windows = (
    df_store_sales
    .withColumn("units_cume_dist", F.cume_dist().over(w))
    .withColumn("units_avg_2_prior", F.avg("units").over(w2))
    .withColumn("revenue_share", F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4))
)

result = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "units_avg_2_prior",
        "revenue_share",
    )
    .orderBy("store_id")
)
