from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.orderBy(F.col("quantity").desc_nulls_last())
w_ordered = Window.partitionBy("product_id", "store_id").orderBy("days_since_last_restock")
w5 = Window.partitionBy("product_id")
w6 = Window.partitionBy("store_id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-09-06").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.last_day("last_restock").alias("last_restock_month_end"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

matched_keys = (
    filtered
    .select("id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id") == 3)
        .select("id", "product_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "*",
        F.dense_rank().over(w).alias("quantity_dense_rank"),
        F.avg("days_since_last_restock").over(w2).alias("product_id_avg_days_since_last_restock"),
        F.dense_rank().over(w3).alias("quantity_dense_rank_2"),
        F.cume_dist().over(w_ordered).alias("days_since_last_restock_cume_dist"),
    )
)

scored_inventory = (
    scored
    .select(
        "*",
        F.round((F.col("quantity_dense_rank_2") / F.sum("quantity_dense_rank_2").over(w5)), 4).alias("quantity_dense_rank_2_share"),
        F.min("days_since_last_restock").over(w6).alias("store_id_min_days_since_last_restock"),
        F.round((F.col("quantity_dense_rank_2") / F.sum("quantity_dense_rank_2").over(w6)), 4).alias("quantity_dense_rank_2_share_2"),
    )
)

filtered_inventory = (
    scored_inventory
    .filter(F.col("quantity_dense_rank_2_share").between(0.21, 0.74))
    .select(
        "*",
        F.round("product_id_avg_days_since_last_restock", 0).alias("product_id_avg_days_since_last_restock_rounded"),
    )
)

joined = (
    filtered_inventory
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.region"), F.col("p.sqft"), F.col("p.opened_date"))
)

result = (
    joined
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock_month_end",
        "days_since_last_restock",
        "product_id_avg_days_since_last_restock",
        "quantity_dense_rank_2",
        "days_since_last_restock_cume_dist",
        "quantity_dense_rank_2_share",
    )
    .orderBy(F.col("days_since_last_restock_cume_dist").desc())
)
