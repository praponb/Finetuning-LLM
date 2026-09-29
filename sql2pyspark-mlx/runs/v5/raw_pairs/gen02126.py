from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").asc_nulls_last(), "id")
w_grp = Window.partitionBy("product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("store_id").orderBy("opened_date", "id")

ranked = (
    df_inventory
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .filter(F.col("d.city") == "Singapore")
    .select("e.*", F.col("d.opened_date"))
)

with_dims = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.brand"), F.col("p.product_name"), F.col("p.category"))
)

scored = (
    with_dims
    .select(
        "*",
        F.avg("quantity").over(w_grp).alias("running_avg_quantity"),
        F.sum("quantity").over(w_grp).alias("running_sum_quantity"),
        F.count("quantity").over(w_grp).alias("running_count_quantity"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

enriched_inventory = (
    scored
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.latest_rank",
        "s.product_name",
        "s.category",
        "s.running_count_quantity",
        "s.row_seq",
        F.col("b.city"),
        F.col("b.sqft"),
    )
)

result = (
    enriched_inventory
    .groupBy(
        F.when(F.col("row_seq") >= 6, "LARGE").otherwise("SMALL").alias("row_seq_tier"),
        F.when(F.col("running_count_quantity") >= 17, "LARGE").otherwise("SMALL").alias("running_count_quantity_tier"),
    )
    .agg(
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.max(F.when(F.col("category") == "Books", F.col("running_count_quantity"))).alias("books_max_running_count_quantity"),
        F.sum(F.when(F.col("category") == "Books", F.col("row_seq")).otherwise(0)).alias("books_row_seq"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("sqft")).otherwise(0)).alias("electronics_sqft"),
    )
    .filter(F.col("books_row_seq") < 1000000)
)
