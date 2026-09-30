from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("rn").desc(), "id")
w3 = Window.partitionBy("product_id", "store_id").orderBy("stock_qty", "id")
w_hist = Window.partitionBy("product_id").orderBy("stock_qty", "id")
w5 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("rn_2", F.row_number().over(w2))
    .filter(F.col("rn_2") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.opened_date"), F.col("d.city"), F.col("d.store_name"))
)

enriched_store_sales = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.stock_qty"), F.col("b.product_name"), F.col("b.brand"))
)

metrics = (
    enriched_store_sales
    .withColumn("stock_qty_bucket", F.ntile(10).over(w3))
    .withColumn("seq_no", F.row_number().over(w_hist))
    .withColumn("running_revenue", F.sum("revenue").over(w5))
)

enriched_store_sales_2 = (
    metrics
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select(
        "e.*",
        F.col("b.region"),
        F.col("b.sqft"),
        F.col("b.city").alias("store_city"),
    )
)

summary = (
    enriched_store_sales_2
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .cube("week_start")
    .agg(
        F.count_if(F.col("region") == "West").alias("west_events"),
        F.avg(F.when(F.col("city") == "New York", F.col("running_revenue"))).alias("new_york_avg_running_revenue"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("west_events") < 1)
)

result = (
    summary
    .select("week_start", "units_quartiles")
    .orderBy("week_start")
)
