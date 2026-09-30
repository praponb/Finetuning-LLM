from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_day", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-04").cast("date"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
        F.round((F.col("quantity") * 1.07), 2).alias("quantity_scaled"),
    )
)

with_windows = (
    cleaned
    .withColumn("last7_min_quantity", F.min("quantity").over(w))
    .withColumn("last7_avg_quantity", F.avg("quantity").over(w))
    .withColumn("last7_max_quantity", F.max("quantity").over(w))
)

cleaned_inventory = (
    with_windows
    .filter(F.col("last_restock_day") <= 33)
    .withColumn("last7_avg_quantity_times_last_restock_day", (F.col("last7_avg_quantity") * F.col("last_restock_day")))
)

valid = (
    cleaned_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-02-12")))
    .withColumn("last7_min_quantity_whole", F.floor("last7_min_quantity"))
)

with_dims = (
    valid
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.last_restock_day",
        "s.last7_min_quantity",
        "s.last7_min_quantity_whole",
        F.col("b.store_name"),
        F.col("b.sqft"),
        F.col("b.city"),
    )
)

kept_keys = (
    with_dims
    .select("id", "store_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity").between(80, 170))
        .select("id", "store_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("city")
    .agg(
        F.percentile_approx("last_restock_day", 0.95).alias("last_restock_day_p95"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
        F.max("last_restock_day").alias("max_last_restock_day"),
        F.percentile_approx("last7_min_quantity_whole", [0.25, 0.5, 0.75]).alias("last7_min_quantity_whole_quartiles"),
    )
    .orderBy(F.col("last_restock_day_p95").desc_nulls_last())
)
