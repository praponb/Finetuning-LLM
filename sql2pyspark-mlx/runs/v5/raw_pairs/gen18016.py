from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("running_avg_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("first_known_quantity", "id")

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-02-21").cast("date")) & (F.col("last_restock") <= "2024-08-07"))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 263, "High").when(F.col("quantity") >= 71, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

scored = (
    base
    .withColumn("running_count_quantity", F.count("quantity").over(w))
    .withColumn("running_avg_quantity", F.avg("quantity").over(w))
    .withColumn("running_max_quantity", F.max("quantity").over(w))
    .withColumn("first_known_quantity", F.first("quantity", ignorenulls=True).over(w))
)

with_windows = (
    scored
    .select(
        "id",
        "store_id",
        "quantity",
        "quantity_level",
        "running_avg_quantity",
        "running_max_quantity",
        F.avg("running_count_quantity").over(w2).alias("running_avg_running_count_quantity"),
        F.max("running_count_quantity").over(w2).alias("running_max_running_count_quantity"),
        F.count("running_count_quantity").over(w2).alias("running_count_running_count_quantity"),
        F.sum("running_count_quantity").over(w2).alias("running_sum_running_count_quantity"),
        F.ntile(4).over(w3).alias("first_known_quantity_bucket"),
    )
)

kept_keys = (
    with_windows
    .select("store_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock").between("2024-01-30", "2024-02-16"))
        .select("store_id", "id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("running_max_quantity") < 130)
    .select(
        "id",
        "store_id",
        "running_avg_quantity",
        "running_avg_running_count_quantity",
        "running_max_running_count_quantity",
        "running_count_running_count_quantity",
        F.round((F.col("running_avg_quantity") / F.col("running_count_running_count_quantity")), 2).alias("running_avg_quantity_per_running_count_running_count_quantity"),
    )
)

result = (
    filtered
    .select(
        "id",
        "running_avg_quantity",
        "running_max_running_count_quantity",
        "running_count_running_count_quantity",
    )
)
