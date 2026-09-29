from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "city",
        F.when(F.col("sqft") >= 13953, "high").when(F.col("sqft") >= 8203, "mid").otherwise("low").alias("sqft_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.max("opened_date").alias("latest_opened_date"),
    )
    .orderBy("latest_opened_date", "city", "sqft_bucket")
    .limit(5)
)
