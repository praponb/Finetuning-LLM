from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity_filled", "id")
w2 = Window.partitionBy("store_id").orderBy("row_num", "id")
w3 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("store_id").orderBy("last_restock", "id")
w_hist = Window.orderBy(F.col("quantity_filled").desc(), "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("quantity_scaled").desc_nulls_last())
w7 = Window.partitionBy("store_id").orderBy("quantity_scaled", "id")

filtered = (
    df_inventory
    .filter(F.col("product_id").isin([5, 10]))
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
        F.round((F.col("quantity") * 1.15), 2).alias("quantity_scaled"),
    )
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "id",
        "store_id",
        "quantity_scaled",
        "row_num",
        "rn",
        F.avg("quantity").over(w3).alias("quantity_avg_5_prior"),
        F.lag("quantity_scaled", 1).over(w4).alias("prev_quantity_scaled"),
        F.ntile(4).over(w_hist).alias("quantity_filled_bucket"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.quantity_scaled",
        "src.rn",
        "src.quantity_filled_bucket",
        F.col("s.sqft"),
    )
)

deduped_inventory = (
    enriched
    .withColumn("rn_2", F.rank().over(w6))
    .filter(F.col("rn_2") == 1)
)

latest_inventory = (
    deduped_inventory
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest_inventory
    .select(
        "id",
        "quantity_scaled",
        "rn",
        "quantity_filled_bucket",
        "rn_2",
        "dedup_rank",
    )
    .orderBy("id", "rn_2")
)
