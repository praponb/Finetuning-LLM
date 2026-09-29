from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("city").orderBy(F.col("row_seq").desc(), "id")
w_ordered = Window.partitionBy("city").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "city",
        F.lag("sqft").over(w).alias("prev_sqft"),
        F.lead("sqft").over(w).alias("next_sqft"),
        F.row_number().over(w).alias("row_seq"),
        F.avg("sqft").over(w2).alias("sqft_avg_6_prior"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity") < 75), "left_semi")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

scored_stores = (
    latest
    .select(
        "*",
        F.max("prev_sqft").over(w_ordered).alias("running_max_prev_sqft"),
        F.count("prev_sqft").over(w_ordered).alias("running_count_prev_sqft"),
        F.avg("prev_sqft").over(w_ordered).alias("running_avg_prev_sqft"),
    )
)

totals = (
    scored_stores
    .groupBy("city")
    .agg(
        F.count("*").alias("cnt"),
        F.percentile_approx("running_count_prev_sqft", [0.25, 0.5, 0.75]).alias("running_count_prev_sqft_quartiles"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("running_count_prev_sqft")).otherwise(0)).alias("berlin_running_count_prev_sqft"),
        F.max(F.when(F.col("city") == "New York", F.col("next_sqft"))).alias("new_york_max_next_sqft"),
    )
    .filter(F.col("berlin_running_count_prev_sqft") < 1000000)
)

result = (
    totals
    .select(
        "city",
        "running_count_prev_sqft_quartiles",
        "berlin_running_count_prev_sqft",
        "new_york_max_next_sqft",
        F.when(F.col("city").rlike("^(New York|Bangkok)$"), "CORE").when(F.col("city").rlike("^(Chiang Mai|Singapore)$"), "PRIORITY").otherwise("GROUP_B").alias("city_zone"),
    )
    .orderBy(F.col("city").desc_nulls_last())
)
