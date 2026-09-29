from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())
w2 = Window.partitionBy("city", "region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        F.dense_rank().over(w_running).alias("sqft_dense_rank"),
        F.sum("sqft").over(w2).alias("running_sqft"),
        F.ntile(4).over(w3).alias("sqft_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("sqft_dense_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("sqft_dense_rank_tier"),
    )
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum("running_sqft").alias("total_running_sqft"),
        F.max("sqft_dense_rank").alias("max_sqft_dense_rank"),
    )
    .orderBy(F.col("num_cities").asc_nulls_last())
)
