from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w_part = Window.partitionBy("store_id").orderBy("latest_rank", "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy("latest_rank", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity").desc_nulls_last())
w6 = Window.partitionBy("product_id").orderBy("quantity_avg_6_prior")

with_dims = (
    df_inventory
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.id", "a.store_id", "a.product_id", "a.quantity", F.col("d.city"))
)

deduped = (
    with_dims
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

metrics = (
    deduped
    .select(
        "id",
        "store_id",
        "product_id",
        F.lag("quantity", 1).over(w_part).alias("prev_quantity"),
        F.avg("quantity").over(w3).alias("store_id_avg_quantity"),
        F.avg("quantity").over(w4).alias("quantity_avg_6_prior"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "store_id",
        "prev_quantity",
        "quantity_avg_6_prior",
        F.percent_rank().over(w5).alias("prev_quantity_percent_rank"),
        F.dense_rank().over(w6).alias("quantity_avg_6_prior_dense_rank"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.region"), F.col("s.opened_date"))
)

matched_keys = (
    joined
    .select("store_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("quantity").between(163, 233))
        .select("store_id", "id")
    )
)

reconciled = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "id",
        "store_id",
        "quantity_avg_6_prior",
        "quantity_avg_6_prior_dense_rank",
        "region",
        "opened_date",
    )
)
