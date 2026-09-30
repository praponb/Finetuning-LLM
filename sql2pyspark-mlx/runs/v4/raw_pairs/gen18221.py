from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("sqft_tier").orderBy(F.col("east_events").desc())

result = (
    df_stores
    .groupBy(
        F.when(F.col("sqft") >= 15666, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.count_if(F.col("region") == "East").alias("east_events"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
    )
    .filter(F.col("distinct_cities") > 1)
    .withColumn("east_events_rank", F.rank().over(w))
)
