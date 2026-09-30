from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("sqft", "id")
w3 = Window.orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft", "id")
w5 = Window.partitionBy("region").orderBy(F.col("sqft").asc_nulls_first())
w6 = Window.partitionBy("city").orderBy("sqft", "id")
w7 = Window.partitionBy("city")
w_hist = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("region", "city").orderBy("opened_date_plus_27d", "id").rowsBetween(-3, -1)
w10 = Window.orderBy("sqft_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("city", "region")

cleaned = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-05"))) & ~F.col("region").isin(["East", "North", "West"]))
    .select("*", F.date_add("opened_date", 27).alias("opened_date_plus_27d"))
)

valid = (
    cleaned
    .filter(F.col("sqft") == 10864)
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        "opened_date_plus_27d",
        F.upper("store_name").alias("store_name_upper"),
    )
)

metrics = (
    valid
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w))
    .withColumn("next_sqft", F.lead("sqft", 2, 0).over(w2))
    .withColumn("sqft_avg_2", F.avg("sqft").over(w3))
    .withColumn("next_sqft_2", F.lead("sqft", 1).over(w4))
    .withColumn("sqft_rank", F.rank().over(w5))
)

with_windows = (
    metrics
    .withColumn("next_next_sqft", F.lead("next_sqft", 1).over(w6))
    .withColumn("next_sqft_2_share", F.round((F.col("next_sqft_2") / F.sum("next_sqft_2").over(w7)), 4))
    .withColumn("last_known_next_sqft", F.last("next_sqft", ignorenulls=True).over(w_hist))
)

result = (
    with_windows
    .withColumn("next_sqft_2_avg_3_prior", F.avg("next_sqft_2").over(w9))
    .withColumn("cumulative_next_sqft", F.sum("next_sqft").over(w10))
    .withColumn("next_next_sqft_share", F.round((F.col("next_next_sqft") / F.sum("next_next_sqft").over(w11)), 4))
)
