from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand")
w3 = Window.partitionBy("brand").orderBy(F.col("running_sum_cost").desc_nulls_last(), "id")
w4 = Window.partitionBy("category").orderBy(F.col("running_sum_cost").desc_nulls_last())
w5 = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(-3, -1)

enriched = (
    df_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.units",
        "s.revenue",
        F.col("p.category"),
        F.col("p.brand"),
        F.col("p.cost"),
    )
)

cleaned = (
    enriched
    .filter(F.col("category").isNotNull())
    .select(
        "id",
        "units",
        "revenue",
        "category",
        "brand",
        "cost",
        F.substring("category", 1, 3).alias("category_prefix"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.count("cost").over(w_hist).alias("running_count_cost"),
        F.sum("cost").over(w_hist).alias("running_sum_cost"),
        F.max("cost").over(w_hist).alias("running_max_cost"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

scored = (
    deduped
    .select(
        "id",
        "units",
        "category",
        "cost_share",
        F.cume_dist().over(w4).alias("running_sum_cost_cume_dist"),
        F.avg("running_sum_cost").over(w5).alias("running_sum_cost_avg_3_prior"),
    )
)

result = (
    scored
    .groupBy("category")
    .agg(
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.percentile_approx("units", 0.5).alias("units_p50"),
    )
    .filter(F.col("electronics_events") >= 1)
)
