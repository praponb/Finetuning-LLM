from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("city", "region").orderBy("days_since_opened_date", "id")
w3 = Window.partitionBy("region")
w_running = Window.partitionBy("city").orderBy(F.col("days_since_opened_date").desc(), "id")
w_ordered = Window.partitionBy("city").orderBy("prev_days_since_opened_date", "id")
w6 = Window.partitionBy("region", "city").orderBy(F.col("region_max_sqft").desc())

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-04-25").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

metrics = (
    valid
    .withColumn("sqft_percent_rank", F.percent_rank().over(w))
    .withColumn("prev_days_since_opened_date", F.lag("days_since_opened_date", 1).over(w2))
)

enriched = (
    metrics
    .withColumn("region_max_sqft", F.max("sqft").over(w3))
    .withColumn("days_since_opened_date_bucket", F.ntile(3).over(w_running))
    .withColumn("next_sqft_percent_rank", F.lead("sqft_percent_rank", 1).over(w_ordered))
)

result = (
    enriched
    .select(
        "*",
        F.lag("days_since_opened_date_bucket", 1).over(w_ordered).alias("prev_days_since_opened_date_bucket"),
        F.rank().over(w6).alias("region_max_sqft_rank"),
    )
)
