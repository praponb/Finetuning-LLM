from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_first())
w_hist = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())

filtered = (
    df_inventory
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)

with_dims = (
    filtered
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.region") == "West")
    .select("e.*", F.col("s.opened_date"))
)

ranked = (
    with_dims
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

remaining_keys = (
    ranked
    .select("store_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id") == 2)
        .select("store_id", "id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("quantity_rank", F.rank().over(w2))
    .withColumn("quantity_dense_rank", F.dense_rank().over(w_hist))
)

result = (
    metrics
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .withColumn("quantity_rank_tier", F.when(F.col("quantity_rank") >= 8, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "quantity_rank_tier"], ["quarter_start"], []],
        "quarter_start",
        "quantity_rank_tier",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity_dense_rank", [0.25, 0.5, 0.75]).alias("quantity_dense_rank_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("quantity_rank_tier").desc())
)
