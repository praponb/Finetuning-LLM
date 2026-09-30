from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy("last3_sum_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("last3_min_quantity", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("product_id").orderBy(F.col("first_known_last3_max_quantity").asc_nulls_last(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("last3_sum_quantity").desc())
w6 = Window.partitionBy("city").orderBy(F.col("first_known_last3_max_quantity_per_last3_max_quantity").desc())

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        F.max("quantity").over(w_recent).alias("last3_max_quantity"),
        F.sum("quantity").over(w_recent).alias("last3_sum_quantity"),
        F.min("quantity").over(w_recent).alias("last3_min_quantity"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.first("last3_max_quantity", ignorenulls=True).over(w2).alias("first_known_last3_max_quantity"),
        F.avg("last3_sum_quantity").over(w3).alias("last3_sum_quantity_avg_6_prior"),
    )
)

ranked = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

base = (
    ranked
    .filter(F.col("store_id").isin([2, 3, 4, 5]))
    .select(
        "id",
        "store_id",
        "last3_max_quantity",
        "last3_sum_quantity",
        "last3_min_quantity",
        "first_known_last3_max_quantity",
        (F.col("first_known_last3_max_quantity") / F.nullif("last3_max_quantity", F.lit(0))).alias("first_known_last3_max_quantity_per_last3_max_quantity"),
        (F.col("last3_sum_quantity") + F.col("last3_sum_quantity_avg_6_prior")).alias("last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "inner")
    .filter(F.col("b.sqft") == 5958)
    .select(
        "a.id",
        "a.store_id",
        "a.last3_sum_quantity",
        "a.first_known_last3_max_quantity",
        "a.first_known_last3_max_quantity_per_last3_max_quantity",
        "a.last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior",
        F.col("b.city"),
        F.col("b.opened_date"),
    )
)

scored = (
    joined
    .select(
        "*",
        F.percent_rank().over(w5).alias("last3_sum_quantity_percent_rank"),
        F.cume_dist().over(w6).alias("first_known_last3_max_quantity_per_last3_max_quantity_cume_dist"),
    )
)

enriched = (
    scored
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.first_known_last3_max_quantity",
        "src.first_known_last3_max_quantity_per_last3_max_quantity",
        "src.last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior",
        "src.city",
        "src.opened_date",
        "src.first_known_last3_max_quantity_per_last3_max_quantity_cume_dist",
        F.col("p.city").alias("store_city"),
        F.col("p.opened_date").alias("store_opened_date"),
    )
)

result = (
    enriched
    .select(
        "id",
        "first_known_last3_max_quantity_per_last3_max_quantity",
        "last3_sum_quantity_plus_last3_sum_quantity_avg_6_prior",
        "city",
        "opened_date",
        "first_known_last3_max_quantity_per_last3_max_quantity_cume_dist",
        "store_city",
    )
    .orderBy("first_known_last3_max_quantity_per_last3_max_quantity_cume_dist")
)
