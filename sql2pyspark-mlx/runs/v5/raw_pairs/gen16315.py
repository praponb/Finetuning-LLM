from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id")
w_recent = Window.partitionBy("product_id").orderBy(F.col("prev_quantity").asc_nulls_first())
w4 = Window.partitionBy("product_id", "store_id").orderBy("seq_no", "id")
w5 = Window.partitionBy("product_id")
w_grp = Window.partitionBy("product_id").orderBy(F.col("next_quantity").desc_nulls_first())
w7 = Window.partitionBy("product_id").orderBy("seq_no_2", "id").rowsBetween(-2, -1)
w8 = Window.partitionBy("product_id").orderBy("next_quantity_rank", "id")
w9 = Window.partitionBy("product_id").orderBy(F.col("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity").asc_nulls_first())
w10 = Window.partitionBy("product_id").orderBy("prev_quantity_rank_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_inventory
    .filter(F.col("store_id").isin([1, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.row_number().over(w).alias("seq_no"),
        F.lead("quantity", 2).over(w2).alias("next_quantity"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "product_id",
        "prev_quantity",
        "next_quantity",
        F.rank().over(w_recent).alias("prev_quantity_rank"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)

scored = (
    enriched
    .withColumn("product_id_min_prev_quantity", F.min("prev_quantity").over(w5))
    .withColumn("next_quantity_rank", F.rank().over(w_grp))
    .withColumn("prev_quantity_rank_avg_2_prior", F.avg("prev_quantity_rank").over(w7))
)

base = (
    scored
    .filter(F.col("prev_quantity_rank").between(6, 7))
    .select(
        "*",
        (F.col("prev_quantity_rank_avg_2_prior") / F.nullif("product_id_min_prev_quantity", F.lit(0))).alias("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity"),
    )
)

scored_inventory = (
    base
    .select(
        "id",
        "prev_quantity",
        "prev_quantity_rank",
        "prev_quantity_rank_avg_2_prior",
        "prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity",
        F.lead("prev_quantity_rank_avg_2_prior", 2).over(w8).alias("next_prev_quantity_rank_avg_2_prior"),
        F.round((F.col("prev_quantity_rank") / F.sum("prev_quantity_rank").over(w5)), 4).alias("prev_quantity_rank_share"),
        F.cume_dist().over(w9).alias("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_cume_dist"),
        F.sum("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity").over(w10).alias("running_prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity"),
        F.last("seq_no_2", ignorenulls=True).over(w10).alias("last_known_seq_no_2"),
    )
)

result = (
    scored_inventory
    .groupBy(
        F.when(F.col("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity") >= 74, "PREMIUM").otherwise("STANDARD").alias("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_tier"),
    )
    .agg(
        F.percentile_approx("last_known_seq_no_2", 0.5).alias("last_known_seq_no_2_p50"),
        F.max("prev_quantity").alias("max_prev_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("prev_quantity_rank_avg_2_prior_per_product_id_min_prev_quantity_tier").desc(),
    )
)
