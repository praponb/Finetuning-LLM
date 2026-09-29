from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.orderBy("sqft_whole", "id")
w3 = Window.orderBy(F.col("sqft_whole").desc(), "id")
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("city", "region").orderBy(F.col("sqft_whole").desc_nulls_last(), "id")
w6 = Window.orderBy(F.col("region_avg_sqft").desc(), "id")
w7 = Window.partitionBy("region", "city").orderBy(F.col("latest_rank").desc(), "id")

cleaned = (
    df_stores
    .filter((F.col("city") == "Bangkok") & (F.col("opened_date") >= F.to_date(F.lit("2023-04-24"))))
    .select(
        "*",
        F.floor("sqft").alias("sqft_whole"),
        F.substring("city", 1, 2).alias("city_prefix"),
    )
)

latest = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    latest
    .withColumn("prev_sqft_whole", F.lag("sqft_whole", 2).over(w2))
    .withColumn("sqft_whole_bucket", F.ntile(10).over(w3))
    .withColumn("region_avg_sqft", F.avg("sqft").over(w4))
)

latest_stores = (
    metrics
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

with_windows = (
    latest_stores
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft_whole",
        "latest_rank",
        "prev_sqft_whole",
        "sqft_whole_bucket",
        "region_avg_sqft",
        "rn",
        F.row_number().over(w6).alias("row_seq"),
        F.row_number().over(w7).alias("row_seq_2"),
    )
)

result = (
    with_windows
    .groupBy("city")
    .pivot("region", ["North", "West"])
    .agg(F.avg("row_seq_2"))
    .select("city", F.col("North").alias("north"), F.col("West").alias("west"))
)
