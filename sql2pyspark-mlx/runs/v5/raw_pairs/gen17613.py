from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("region").orderBy("last7_sum_sqft", "id")
w4 = Window.partitionBy("city").orderBy("last7_max_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2023-08-02").cast("date")) & (~F.col("region").eqNullSafe(F.lit("North"))))
    .select(
        "*",
        F.floor("sqft").alias("sqft_whole"),
        F.initcap(F.trim("region")).alias("region_title"),
    )
)

with_windows = (
    filtered
    .withColumn("last7_max_sqft", F.max("sqft").over(w))
    .withColumn("last7_min_sqft", F.min("sqft").over(w))
    .withColumn("last7_count_sqft", F.count("sqft").over(w))
    .withColumn("last7_sum_sqft", F.sum("sqft").over(w))
    .withColumn("sqft_whole_avg_5_prior", F.avg("sqft_whole").over(w2))
)

eligible = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") >= "2025-04-01"), "left_semi")
)

deduped = (
    eligible
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    deduped
    .filter(F.col("last7_max_sqft") <= 4500)
    .select(
        "*",
        F.ntile(4).over(w3).alias("last7_sum_sqft_bucket"),
        F.last("sqft_whole", ignorenulls=True).over(w4).alias("last_known_sqft_whole"),
    )
)

result = (
    enriched
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft_whole",
        "region_title",
        "last7_min_sqft",
        "last7_count_sqft",
        "sqft_whole_avg_5_prior",
        F.when(F.col("city").rlike("^(New York|Tokyo)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("city_zone"),
    )
    .orderBy(F.col("id").desc())
)
