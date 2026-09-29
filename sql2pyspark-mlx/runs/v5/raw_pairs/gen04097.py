from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("region", "city").orderBy("opened_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("city").orderBy("sqft", "id")

enriched = (
    df_stores
    .filter(~F.col("region").isin(["South", "West"]))
    .select(
        "*",
        F.avg("sqft").over(w_running).alias("sqft_avg_6_prior"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(5).over(w3).alias("sqft_bucket"),
    )
)

result = (
    enriched
    .groupBy("city", F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count_if(F.col("city") == "Berlin").alias("berlin_events"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.countDistinct("city").alias("distinct_cities"),
    )
    .filter(F.col("num_cities") > 1)
)
