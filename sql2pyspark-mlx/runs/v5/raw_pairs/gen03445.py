from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_region").orderBy("sqft", "id")
w3 = Window.partitionBy("store_region", "region").orderBy("last_restock", "id").rowsBetween(-3, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

joined = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.region"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .filter(~F.col("d.region").isin(["Central", "South"]))
    .select(
        "s.id",
        "s.store_id",
        "s.last_restock",
        "s.region",
        F.col("d.sqft"),
        F.col("d.region").alias("store_region"),
    )
)

scored = (
    with_dims
    .select(
        "id",
        "sqft",
        F.row_number().over(w).alias("event_seq"),
        F.lag("sqft", 1, 0).over(w2).alias("prev_sqft"),
        F.avg("sqft").over(w3).alias("sqft_avg_3"),
        F.last("sqft", ignorenulls=True).over(w4).alias("last_known_sqft"),
    )
)

cleaned = (
    scored
    .filter(F.col("sqft").between(9538, 14236))
    .select(
        "id",
        "sqft",
        "event_seq",
        "prev_sqft",
        "sqft_avg_3",
        "last_known_sqft",
        (F.col("sqft_avg_3") * F.col("event_seq")).alias("sqft_avg_3_times_event_seq"),
        F.least("last_known_sqft", "sqft").alias("least_last_known_sqft_sqft"),
    )
)

valid = (
    cleaned
    .filter(F.col("last_known_sqft") < 9887)
    .withColumn("greatest_sqft_sqft_avg_3", F.greatest("sqft", "sqft_avg_3"))
    .withColumn("adjusted_event_seq", F.round((F.col("event_seq") * 1.07), 2))
)

result = (
    valid
    .groupBy(
        F.when(F.col("sqft") >= 14476, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        F.when(F.col("sqft_avg_3") >= 16122, "LARGE").otherwise("SMALL").alias("sqft_avg_3_tier"),
    )
    .agg(
        F.percentile_approx("sqft_avg_3", [0.25, 0.5, 0.75]).alias("sqft_avg_3_quartiles"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
)
