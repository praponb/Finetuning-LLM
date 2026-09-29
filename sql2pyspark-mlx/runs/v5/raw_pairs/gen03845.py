from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("quantity_whole", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy("quantity_whole_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-03-15")))
    .withColumn("quantity_whole", F.ceil("quantity"))
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

with_windows = (
    valid
    .filter(F.col("quantity_whole") != 37)
    .select(
        "id",
        "quantity_whole",
        "quantity_num",
        F.sum("quantity").over(w).alias("running_quantity"),
        F.avg("quantity_whole").over(w2).alias("quantity_whole_avg_2"),
    )
)

with_windows_inventory = (
    with_windows
    .select(
        "*",
        F.min("quantity_whole_avg_2").over(w3).alias("running_min_quantity_whole_avg_2"),
        F.max("quantity_whole_avg_2").over(w3).alias("running_max_quantity_whole_avg_2"),
        F.sum("quantity_whole_avg_2").over(w3).alias("running_sum_quantity_whole_avg_2"),
        F.avg("quantity_whole_avg_2").over(w3).alias("running_avg_quantity_whole_avg_2"),
        F.first("quantity_whole_avg_2", ignorenulls=True).over(w4).alias("first_known_quantity_whole_avg_2"),
    )
)

kept_keys = (
    with_windows_inventory
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 4, 5]))
        .select("id")
    )
)

kept_rows = (
    with_windows_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .groupBy(
        F.when(F.col("running_avg_quantity_whole_avg_2") >= 86, "HIGH").otherwise("LOW").alias("running_avg_quantity_whole_avg_2_tier"),
    )
    .agg(
        F.min("running_max_quantity_whole_avg_2").alias("min_running_max_quantity_whole_avg_2"),
        F.percentile_approx("running_quantity", 0.9).alias("running_quantity_p90"),
    )
    .filter(F.col("running_quantity_p90") < 1000000)
)

result = (
    grouped
    .select("running_avg_quantity_whole_avg_2_tier")
    .orderBy(F.col("running_avg_quantity_whole_avg_2_tier").desc_nulls_last())
)
