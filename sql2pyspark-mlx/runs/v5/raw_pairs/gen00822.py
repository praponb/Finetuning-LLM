from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region", "city")
w5 = Window.orderBy(F.col("last_known_sqft").desc(), "id")
w6 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("region").orderBy(F.col("region_max_sqft").desc_nulls_first())

cleaned = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-09-24").cast("date"))
    .withColumn("city_group", F.when(F.col("city").isin(["Tokyo", "Bangkok"]), "Tier 1").otherwise("Other"))
)

with_windows = (
    cleaned
    .withColumn("region_max_sqft", F.max("sqft").over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w2))
    .withColumn("last_known_sqft", F.last("sqft", ignorenulls=True).over(w3))
)

metrics = (
    with_windows
    .select(
        "*",
        F.count("last_known_sqft").over(w4).alias("region_count_last_known_sqft"),
        F.ntile(4).over(w5).alias("last_known_sqft_bucket"),
        F.avg("first_known_sqft").over(w6).alias("first_known_sqft_avg_2_prior"),
    )
)

deduped = (
    metrics
    .withColumn("latest_rank", F.rank().over(w7))
    .filter(F.col("latest_rank") == 1)
)

base = (
    deduped
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-04-20"))) & (~F.col("region").eqNullSafe(F.lit("East"))))
    .select(
        "*",
        (F.col("latest_rank") * F.col("last_known_sqft")).alias("latest_rank_times_last_known_sqft"),
        F.ceil("first_known_sqft").alias("first_known_sqft_whole"),
        F.coalesce("region", F.lit("NONE")).alias("resolved_region"),
    )
)

result = (
    base
    .select(
        "id",
        "opened_date",
        "sqft",
        "city_group",
        "region_max_sqft",
        "first_known_sqft",
        "region_count_last_known_sqft",
        "first_known_sqft_avg_2_prior",
        "latest_rank_times_last_known_sqft",
    )
    .orderBy("region_max_sqft")
)
