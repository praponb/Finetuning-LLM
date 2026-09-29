from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w3 = Window.orderBy(F.col("cost").desc())

enriched = (
    df_inventory
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.last_restock",
        F.col("d.opened_date"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked = (
    deduped
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
    .select("id", "product_id", "last_restock")
)

cleaned = (
    ranked
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-04-11"))) & (F.year("last_restock") == 2024))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

remaining_keys = (
    cleaned
    .select("id", "product_id")
    .intersect(
        df_inventory
        .filter(F.col("quantity") == 185)
        .select("id", "product_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.cost") < 520)
    .select(
        "e.id",
        "e.product_id",
        "e.last_restock",
        F.col("b.brand"),
        F.col("b.cost"),
        F.col("b.category"),
    )
)

scored = joined.select("*", F.cume_dist().over(w3).alias("cost_cume_dist"))

result = (
    scored
    .withColumn("week_start", F.date_trunc("WEEK", "last_restock"))
    .withColumn("cost_tier", F.when(F.col("cost") >= 690, "LARGE").otherwise("SMALL"))
    .rollup("week_start", "cost_tier")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("cost_cume_dist", 0.5).alias("cost_cume_dist_p50"),
        F.avg("cost").alias("avg_cost"),
        F.grouping_id().alias("gid"),
    )
)
