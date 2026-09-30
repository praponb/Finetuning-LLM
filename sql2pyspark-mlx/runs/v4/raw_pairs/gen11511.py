from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc_nulls_first(), "id")

deduped = (
    df_stores
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.when(F.col("sqft") >= 16131, "HIGH").otherwise("LOW").alias("sqft_tier"),
    )
    .agg(
        F.count_if(F.col("region") == "South").alias("south_events"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .filter(F.col("avg_sqft") < 0)
    .orderBy("week_start")
)
