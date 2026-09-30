from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("cumulative_quantity").desc())
w4 = Window.partitionBy("product_id", "city").orderBy(F.col("cumulative_quantity").desc())
w5 = Window.partitionBy("store_id").orderBy(F.col("cumulative_quantity_dense_rank").desc())
w6 = Window.partitionBy("product_id").orderBy("cumulative_quantity_dense_rank_2")
w7 = Window.partitionBy("city")
w8 = Window.partitionBy("product_id")

scored = (
    df_inventory
    .withColumn("quantity_avg_3", F.avg("quantity").over(w_grp))
    .withColumn("cumulative_quantity", F.sum("quantity").over(w2))
)

matched_keys = (
    scored
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") >= "2024-01-01")
        .select("product_id", "store_id")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"))
)

metrics = (
    with_dims
    .withColumn("cumulative_quantity_dense_rank", F.dense_rank().over(w3))
    .withColumn("cumulative_quantity_dense_rank_2", F.dense_rank().over(w4))
)

result = (
    metrics
    .select(
        "*",
        F.rank().over(w4).alias("cumulative_quantity_rank"),
        F.dense_rank().over(w5).alias("cumulative_quantity_dense_rank_dense_rank"),
        F.dense_rank().over(w6).alias("cumulative_quantity_dense_rank_2_dense_rank"),
        F.round((F.col("cumulative_quantity_dense_rank") / F.sum("cumulative_quantity_dense_rank").over(w7)), 4).alias("cumulative_quantity_dense_rank_share"),
        F.round((F.col("cumulative_quantity_dense_rank") / F.sum("cumulative_quantity_dense_rank").over(w8)), 4).alias("cumulative_quantity_dense_rank_share_2"),
    )
)
