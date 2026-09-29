from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("activity_month").orderBy(F.col("sqft_share_per_north").desc())

metrics = (
    df_stores
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
        F.sum("sqft").over(w4).alias("running_sqft"),
    )
)

result = (
    metrics
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("activity_month"),
        F.when(F.col("cumulative_sqft") >= 87393, "PREMIUM").otherwise("STANDARD").alias("cumulative_sqft_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(F.sum("sqft_share") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("sqft_share_per_north"),
    )
    .withColumn("sqft_share_per_north_rank", F.rank().over(w5))
    .orderBy(F.col("cumulative_sqft_tier").desc())
)
