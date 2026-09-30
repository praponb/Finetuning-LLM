from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first())
w2 = Window.partitionBy("region")
w3 = Window.partitionBy("city")

eligible = (
    df_stores
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.month(F.col("x.order_date")).isin([4, 7, 8])), "left_semi")
)

ranked = (
    eligible
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

remaining_keys = (
    ranked
    .select("region", "city")
    .intersectAll(
        df_stores
        .filter(F.col("opened_date").between("2024-04-18", "2024-10-01"))
        .select("region", "city")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("region").isNotNull() & (F.col("region").eqNullSafe(F.lit("South"))))
    .withColumn("row_num_per_sqft", F.round((F.col("row_num") / F.col("sqft")), 2))
)

enriched = (
    filtered
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w2)), 4).alias("sqft_share"),
        F.stddev_samp("sqft").over(w3).alias("city_stddev_samp_sqft"),
        F.round((F.col("sqft") / F.sum("sqft").over(w3)), 4).alias("sqft_share_2"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.percentile_approx("row_num_per_sqft", [0.25, 0.5, 0.75]).alias("row_num_per_sqft_quartiles"),
        F.count(F.when(F.col("region") == "South", 1)).alias("south_rows"),
    )
    .orderBy(F.col("central_events").desc_nulls_last())
)
