from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc(), "id")
w2 = Window.partitionBy("city")

enriched = (
    df_stores
    .select(
        "*",
        F.ntile(4).over(w).alias("sqft_bucket"),
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("sqft_bucket") >= 7, "LARGE").otherwise("SMALL").alias("sqft_bucket_tier"),
        F.when(F.col("sqft_share") >= 0.86, "PREMIUM").otherwise("STANDARD").alias("sqft_share_tier"),
    )
    .agg(
        F.round(F.avg("sqft_share"), 2).alias("avg_sqft_share"),
        F.countDistinct("region").alias("distinct_regions"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
    )
    .filter(F.col("avg_sqft") > 0)
)
