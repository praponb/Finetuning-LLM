from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy(F.col("last7_avg_revenue").desc_nulls_first())

metrics = (
    df_store_sales
    .withColumn("last7_sum_revenue", F.sum("revenue").over(w_running))
    .withColumn("last7_count_revenue", F.count("revenue").over(w_running))
    .withColumn("last7_max_revenue", F.max("revenue").over(w_running))
    .withColumn("last7_avg_revenue", F.avg("revenue").over(w_running))
)

matched_keys = (
    metrics
    .select("id")
    .exceptAll(
        df_store_sales
        .filter(F.col("units") <= 39)
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("sale_date") > F.lit("2023-02-25").cast("date"))
    .select(
        "*",
        F.round((F.col("last7_count_revenue") / F.col("last7_avg_revenue")), 2).alias("last7_count_revenue_per_last7_avg_revenue"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.category").isin(["Fashion", "Books", "Grocery"]))
    .select("s.*", F.col("p.stock_qty"), F.col("p.launch_date"), F.col("p.price"))
)

enriched = (
    joined
    .select(
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "last7_sum_revenue",
        "last7_max_revenue",
        "last7_avg_revenue",
        F.cume_dist().over(w2).alias("last7_avg_revenue_cume_dist"),
    )
)

result = (
    enriched
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .groupBy(F.col("d.brand").alias("brand"), F.col("d.category").alias("category"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("e.last7_sum_revenue")).alias("max_last7_sum_revenue"),
    )
    .orderBy("category")
)
