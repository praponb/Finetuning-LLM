from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("cumulative_quantity", "id")
w5 = Window.partitionBy("store_id").orderBy("cumulative_quantity", "id").rowsBetween(-6, -1)
w6 = Window.orderBy("quantity", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("product_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("last4_min_quantity", "id").rowsBetween(-3, -1)
w9 = Window.partitionBy("product_id").orderBy(F.col("prev_last4_min_quantity").desc())
w_hist = Window.partitionBy("category")
w11 = Window.partitionBy("product_id", "category").orderBy("cumulative_quantity_avg_6_prior", "id").rowsBetween(-2, -1)
w12 = Window.partitionBy("product_id", "category").orderBy("category_min_last4_min_quantity_avg_6", "id").rowsBetween(-2, Window.currentRow)
w13 = Window.partitionBy("product_id").orderBy("last4_min_quantity_avg_6", "id")

ranked = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .select(
        "*",
        F.count("quantity").over(w2).alias("last4_count_quantity"),
        F.min("quantity").over(w2).alias("last4_min_quantity"),
        F.sum("quantity").over(w2).alias("last4_sum_quantity"),
        F.sum("quantity").over(w3).alias("cumulative_quantity"),
    )
)

scored = (
    with_windows
    .filter(F.col("last4_min_quantity") >= 100)
    .select(
        "id",
        "product_id",
        "last_restock",
        "last4_min_quantity",
        F.lag("last4_min_quantity", 1).over(w4).alias("prev_last4_min_quantity"),
        F.avg("cumulative_quantity").over(w5).alias("cumulative_quantity_avg_6_prior"),
        F.avg("last4_min_quantity").over(w6).alias("last4_min_quantity_avg_6"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.category"), F.col("p.launch_date"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.sum("last4_min_quantity_avg_6").over(w_grp).alias("running_last4_min_quantity_avg_6"),
        F.avg("prev_last4_min_quantity").over(w8).alias("prev_last4_min_quantity_avg_3_prior"),
        F.rank().over(w9).alias("prev_last4_min_quantity_rank"),
        F.min("last4_min_quantity_avg_6").over(w_hist).alias("category_min_last4_min_quantity_avg_6"),
        F.avg("last4_min_quantity").over(w11).alias("last4_min_quantity_avg_2_prior"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select(
        "a.*",
        F.col("p.launch_date").alias("product_launch_date"),
        F.col("p.cost"),
        F.col("p.stock_qty"),
    )
)

enriched_inventory = (
    joined
    .withColumn("prev_last4_min_quantity_avg_3_prior_avg_2", F.avg("prev_last4_min_quantity_avg_3_prior").over(w12))
    .withColumn("next_prev_last4_min_quantity_avg_3_prior", F.lead("prev_last4_min_quantity_avg_3_prior", 1).over(w13))
)

result = (
    enriched_inventory
    .groupBy(
        F.when(F.col("last4_min_quantity_avg_2_prior") >= 251, "HIGH").otherwise("LOW").alias("last4_min_quantity_avg_2_prior_tier"),
        F.when(F.col("cost") >= 550, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
        "category",
    )
    .agg(
        F.percentile_approx("prev_last4_min_quantity_avg_3_prior_avg_2", 0.95).alias("prev_last4_min_quantity_avg_3_prior_avg_2_p95"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sort_array(F.collect_set("category")).alias("category_list_2"),
    )
    .filter(F.col("prev_last4_min_quantity_avg_3_prior_avg_2_p95") >= 0)
    .orderBy(F.col("category").desc())
)
