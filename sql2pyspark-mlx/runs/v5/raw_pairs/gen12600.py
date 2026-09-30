from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_last(), "id")

ranked = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("region")
    .agg(
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.max(F.when(F.col("region") == "Central", F.col("sqft"))).alias("central_max_sqft"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("region").desc_nulls_first())
)
