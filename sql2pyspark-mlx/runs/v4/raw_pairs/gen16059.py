from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-5, -1)
w_part = Window.partitionBy("city").orderBy(F.col("region_length").desc_nulls_last())
w3 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city")
w5 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w7 = Window.orderBy("first_known_sqft", "id")

cleaned = (
    df_stores
    .filter(F.year("opened_date") >= 2023)
    .select(
        "id",
        "store_name",
        "city",
        "opened_date",
        "sqft",
        F.length("region").alias("region_length"),
    )
)

eligible = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.opened_date")), "left_semi")
)

enriched = (
    eligible
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w))
    .withColumn("region_length_cume_dist", F.cume_dist().over(w_part))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w3))
)

enriched_stores = (
    enriched
    .select(
        "*",
        F.sum("region_length_cume_dist").over(w4).alias("city_sum_region_length_cume_dist"),
        F.first("sqft", ignorenulls=True).over(w5).alias("first_known_sqft_2"),
        F.ntile(3).over(w_grp).alias("sqft_bucket"),
        F.lead("region_length_cume_dist", 1).over(w7).alias("next_region_length_cume_dist"),
    )
)

result = (
    enriched_stores
    .groupBy("city")
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
    )
    .filter(F.col("event_count") < 1)
)
