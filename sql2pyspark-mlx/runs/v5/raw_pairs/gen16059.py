from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("product_id", "store_id")
w_running = Window.partitionBy("store_id", "product_id").orderBy("store_id_avg_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id")
w_grp = Window.partitionBy("store_id").orderBy(F.col("store_id_avg_revenue").desc())
w6 = Window.orderBy("revenue_share_2", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("store_id_avg_revenue_rank", "id")

metrics = (
    df_store_sales
    .filter(F.col("revenue").isNotNull())
    .select(
        "*",
        F.avg("revenue").over(w).alias("store_id_avg_revenue"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
    )
)

metrics_store_sales = (
    metrics
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "store_id_avg_revenue",
        "revenue_share",
        F.first("revenue", ignorenulls=True).over(w_running).alias("first_known_revenue"),
        F.round((F.col("units") / F.sum("units").over(w4)), 4).alias("units_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share_2"),
        F.rank().over(w_grp).alias("store_id_avg_revenue_rank"),
    )
)

metrics_store_sales_2 = (
    metrics_store_sales
    .select(
        "id",
        "store_id",
        "revenue",
        "store_id_avg_revenue",
        "units_share",
        "revenue_share_2",
        F.avg("revenue").over(w6).alias("revenue_avg_3"),
        F.ntile(4).over(w7).alias("store_id_avg_revenue_rank_bucket"),
    )
)

matched_keys = (
    metrics_store_sales_2
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.col("units") >= 23)
        .select("store_id")
    )
)

reconciled = (
    metrics_store_sales_2
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("store_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2, 8, 9]))
        .select("store_id")
    )
)

reconciled_store_sales = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

joined = (
    reconciled_store_sales
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.revenue",
        "e.store_id_avg_revenue",
        "e.revenue_share_2",
        "e.revenue_avg_3",
        F.col("p.city"),
    )
)

result = (
    joined
    .groupBy("store_id")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_rows"),
        F.avg("revenue_share_2").alias("avg_revenue_share_2"),
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("revenue_avg_3"))).alias("chiang_mai_total_revenue_avg_3"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
    )
)
