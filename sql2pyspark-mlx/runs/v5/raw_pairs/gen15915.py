from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity_rounded", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("quantity_whole", "id").rowsBetween(-6, -1)
w_recent = Window.orderBy("next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("quantity_rounded_bucket", "id").rowsBetween(-2, -1)
w6 = Window.orderBy("running_max_next_quantity", "id")
w7 = Window.orderBy("running_count_next_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-30")))
    .select(
        "*",
        F.floor("quantity").alias("quantity_whole"),
        F.round("quantity", 2).alias("quantity_rounded"),
    )
)

enriched = (
    valid
    .filter(F.col("store_id") == 1)
    .select(
        "id",
        "last_restock",
        F.lead("quantity", 1).over(w).alias("next_quantity"),
        F.ntile(10).over(w2).alias("quantity_rounded_bucket"),
        F.avg("quantity_whole").over(w3).alias("quantity_whole_avg_6_prior"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "last_restock",
        "next_quantity",
        "quantity_rounded_bucket",
        F.avg("next_quantity").over(w_recent).alias("running_avg_next_quantity"),
        F.min("next_quantity").over(w_recent).alias("running_min_next_quantity"),
        F.count("next_quantity").over(w_recent).alias("running_count_next_quantity"),
        F.max("next_quantity").over(w_recent).alias("running_max_next_quantity"),
    )
)

valid_inventory = (
    metrics
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-05-28")))
    .select(
        "*",
        (F.col("running_max_next_quantity") * F.col("next_quantity")).alias("running_max_next_quantity_times_next_quantity"),
    )
)

with_windows = (
    valid_inventory
    .select(
        "*",
        F.avg("running_max_next_quantity_times_next_quantity").over(w_ordered).alias("running_max_next_quantity_times_next_quantity_avg_2_prior"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.last("running_max_next_quantity_times_next_quantity_avg_2_prior", ignorenulls=True).over(w7).alias("last_known_running_max_next_quantity_times_next_quantity_avg_2_prior"),
        F.first("running_max_next_quantity_times_next_quantity", ignorenulls=True).over(w8).alias("first_known_running_max_next_quantity_times_next_quantity"),
    )
)

matched_keys = (
    scored
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([1, 7, 8, 10]))
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

daily = (
    verified
    .withColumn("running_max_next_quantity_times_next_quantity_tier", F.when(F.col("running_max_next_quantity_times_next_quantity") >= 83, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["seq_no", "running_max_next_quantity_times_next_quantity_tier"], ["seq_no"]],
        "seq_no",
        "running_max_next_quantity_times_next_quantity_tier",
    )
    .agg(
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity"),
        F.max("running_min_next_quantity").alias("max_running_min_next_quantity"),
        F.count("*").alias("cnt"),
    )
)

result = (
    daily
    .select("seq_no", "max_running_min_next_quantity")
    .orderBy(F.col("max_running_min_next_quantity").desc(), "seq_no")
)
