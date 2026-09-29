from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("days_since_last_restock", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id", "category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("store_id_stddev_samp_days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy("quantity_bucket", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy("category", "product_category").orderBy("price_avg_5_prior", "id")
w8 = Window.partitionBy("product_category").orderBy("last_known_quantity", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("product_id").orderBy("cumulative_price", "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-09-28")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 183, "High").when(F.col("quantity") >= 32, "Medium").otherwise("Low").alias("quantity_band"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

scored = (
    valid
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_band",
        F.lead("quantity", 1, 0).over(w).alias("next_quantity"),
        F.stddev_samp("days_since_last_restock").over(w2).alias("store_id_stddev_samp_days_since_last_restock"),
        F.ntile(3).over(w3).alias("quantity_bucket"),
    )
)

with_dims = (
    scored
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"), F.col("p.price"))
)

scored_inventory = (
    with_dims
    .select(
        "id",
        "product_id",
        "category",
        F.sum("price").over(w4).alias("cumulative_price"),
        F.last("quantity", ignorenulls=True).over(w5).alias("last_known_quantity"),
        F.avg("price").over(w6).alias("price_avg_5_prior"),
    )
)

filtered = (
    scored_inventory
    .filter((~F.col("category").eqNullSafe(F.lit("Electronics"))) & (F.col("last_known_quantity") == 192))
    .select(
        "*",
        (F.col("cumulative_price") + F.col("last_known_quantity")).alias("cumulative_price_plus_last_known_quantity"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.category").alias("product_category"))
)

scored_inventory_2 = (
    joined
    .filter(F.col("last_known_quantity") == 218)
    .select(
        "id",
        "category",
        "cumulative_price",
        "last_known_quantity",
        F.lag("cumulative_price_plus_last_known_quantity", 1).over(w_grp).alias("prev_cumulative_price_plus_last_known_quantity"),
        F.avg("last_known_quantity").over(w8).alias("last_known_quantity_avg_3_prior"),
        F.lead("price_avg_5_prior", 1).over(w9).alias("next_price_avg_5_prior"),
    )
)

result = (
    scored_inventory_2
    .groupBy(
        "category",
        F.when(F.col("prev_cumulative_price_plus_last_known_quantity") >= 76, "LARGE").otherwise("SMALL").alias("prev_cumulative_price_plus_last_known_quantity_tier"),
    )
    .agg(
        F.min("prev_cumulative_price_plus_last_known_quantity").alias("min_prev_cumulative_price_plus_last_known_quantity"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sort_array(F.collect_set("category")).alias("category_list_2"),
    )
    .orderBy("category")
)
