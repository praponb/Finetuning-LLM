from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(-5, Window.currentRow)

enriched = (
    df_stores
    .withColumn("sqft_share", F.round((F.col("sqft") / F.sum("sqft").over(w)), 4))
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w2))
    .withColumn("sqft_avg_5", F.avg("sqft").over(w3))
)

result = (
    enriched
    .withColumn("activity_month", F.date_trunc("MONTH", "opened_date"))
    .cube("city", "activity_month", "region")
    .agg(
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
        F.count("*").alias("num_rows"),
        F.grouping("city").alias("grouping_level"),
    )
    .filter(F.col("num_rows") > 1)
)
