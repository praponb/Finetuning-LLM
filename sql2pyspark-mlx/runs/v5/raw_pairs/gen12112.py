from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("region", "city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("region").orderBy("running_count_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("running_count_sqft", "id").rowsBetween(-3, -1)

valid = (
    df_stores
    .filter((~F.col("region").eqNullSafe(F.lit("Central"))) & (F.col("opened_date") > F.lit("2023-07-22").cast("date")))
    .withColumn("city_group", F.when(F.col("city").isin(["Tokyo", "New York"]), "Group A").otherwise("Other"))
    .withColumn("region_filled", F.coalesce("region", F.lit("OTHER")))
)

with_windows = (
    valid
    .filter(F.col("region").isin(["West", "Central"]))
    .select(
        "id",
        "city",
        "region",
        "sqft",
        "region_filled",
        F.sum("sqft").over(w_hist).alias("running_sum_sqft"),
        F.max("sqft").over(w_hist).alias("running_max_sqft"),
        F.count("sqft").over(w_hist).alias("running_count_sqft"),
        F.min("sqft").over(w_hist).alias("running_min_sqft"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.units") < 20), "left_anti")
)

eligible_stores = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.sale_date").between("2023-03-16", "2023-12-01")), "left_semi")
)

eligible_stores_2 = (
    eligible_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([1, 3, 4, 6])), "left_anti")
)

latest_match = (
    eligible_stores_2
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.sqft",
        "s.running_max_sqft",
        "s.running_count_sqft",
        F.col("c.status"),
    )
)

scored = (
    latest_match
    .withColumn("running_avg_sqft", F.avg("sqft").over(w_ordered))
    .withColumn("running_count_sqft_2", F.count("sqft").over(w_ordered))
    .withColumn("running_min_sqft", F.min("sqft").over(w_ordered))
    .withColumn("running_count_sqft_avg_3_prior", F.avg("running_count_sqft").over(w4))
)

result = (
    scored
    .cube("status")
    .agg(
        F.min("running_count_sqft_avg_3_prior").alias("min_running_count_sqft_avg_3_prior"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.percentile_approx("running_max_sqft", [0.25, 0.5, 0.75]).alias("running_max_sqft_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("north_count").desc())
)
