from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-5, -1)
w_running = Window.orderBy("revenue", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .filter(F.col("b.opened_date") < "2023-10-14")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.revenue",
        F.col("b.sqft"),
    )
)

with_windows = (
    with_dims
    .select(
        "*",
        F.avg("sqft").over(w).alias("sqft_avg_5_prior"),
        F.avg("revenue").over(w_running).alias("revenue_avg_2_prior"),
        F.last("revenue", ignorenulls=True).over(w3).alias("last_known_revenue"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue",
        "sqft",
        "revenue_avg_2_prior",
        "last_known_revenue",
        F.round((F.col("revenue_avg_2_prior") / F.sum("revenue_avg_2_prior").over(w4)), 4).alias("revenue_avg_2_prior_share"),
        F.sum("sqft").over(w5).alias("running_sqft"),
        F.sum("revenue").over(w6).alias("cumulative_revenue"),
    )
)

cleaned = (
    enriched
    .filter(F.col("revenue_avg_2_prior") > 1938)
    .withColumn("revenue_avg_2_prior_share_whole", F.floor("revenue_avg_2_prior_share"))
)

result = (
    cleaned
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.category").alias("category"), F.col("p.brand").alias("brand"))
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("e.cumulative_revenue")).alias("total_cumulative_revenue"),
        F.round(F.sum(F.col("e.revenue_avg_2_prior")), 2).alias("total_revenue_avg_2_prior"),
    )
)
