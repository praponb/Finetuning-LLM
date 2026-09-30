from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_ordered_2 = Window.orderBy("quantity", "id")
w_grp = Window.orderBy("last_restock", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w6 = Window.orderBy(F.col("quantity_bucket").desc_nulls_last())
w7 = Window.orderBy(F.col("quantity_bucket").desc())

enriched = (
    df_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        F.lead("quantity", 2).over(w_ordered).alias("next_quantity"),
        F.lag("quantity", 2).over(w_ordered_2).alias("prev_quantity"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_5"),
        F.sum("quantity").over(w4).alias("running_quantity"),
        F.ntile(4).over(w5).alias("quantity_bucket"),
    )
)

matched_keys = (
    enriched
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 4]))
        .select("id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

matched_keys_2 = (
    verified
    .select("id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 3, 8]))
        .select("id")
    )
)

verified_inventory = (
    verified
    .alias("s")
    .join(matched_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched_inventory = (
    verified_inventory
    .filter(F.col("prev_quantity") < 120)
    .select(
        "quantity",
        "prev_quantity",
        F.rank().over(w6).alias("quantity_bucket_rank"),
        F.percent_rank().over(w7).alias("quantity_bucket_percent_rank"),
    )
)

result = (
    enriched_inventory
    .groupBy(
        F.when(F.col("quantity") >= 203, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity_bucket_percent_rank", [0.25, 0.5, 0.75]).alias("quantity_bucket_percent_rank_quartiles"),
        F.round(F.avg("prev_quantity"), 2).alias("avg_prev_quantity"),
        F.round(F.avg("quantity_bucket_percent_rank"), 2).alias("avg_quantity_bucket_percent_rank"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.sum("quantity_bucket_percent_rank").alias("total_quantity_bucket_percent_rank"),
    )
    .filter(F.col("avg_prev_quantity") < 1000000)
    .orderBy(F.col("quantity_bit_and").asc_nulls_last())
)
