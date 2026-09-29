from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").desc_nulls_first(), "id")
w2 = Window.partitionBy("category", "city").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w_recent = Window.partitionBy("city", "category").orderBy("running_avg_stock_qty", "id")
w5 = Window.partitionBy("category", "city").orderBy(F.col("running_avg_stock_qty").desc())
w6 = Window.partitionBy("category").orderBy("rating", "id")
w7 = Window.orderBy("running_count_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.date_format("launch_date", "yyyy-MM-dd").alias("launch_date_label"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    deduped
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.city"), F.col("b.supplier_name"), F.col("b.rating"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.min("stock_qty").over(w2).alias("running_min_stock_qty"),
        F.count("stock_qty").over(w2).alias("running_count_stock_qty"),
        F.avg("stock_qty").over(w2).alias("running_avg_stock_qty"),
        F.max("stock_qty").over(w2).alias("running_max_stock_qty"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= "2024-02-01"), "left_anti")
)

with_windows_products = (
    kept
    .filter(~F.col("city").isin(["Chiang Mai", "Singapore", "Berlin"]))
    .select(
        "*",
        F.row_number().over(w_grp).alias("seq_no"),
        F.ntile(3).over(w_recent).alias("running_avg_stock_qty_bucket"),
        F.cume_dist().over(w5).alias("running_avg_stock_qty_cume_dist"),
    )
)

metrics = (
    with_windows_products
    .select(
        "*",
        F.lead("stock_qty").over(w6).alias("next_stock_qty"),
        F.lag("stock_qty").over(w6).alias("prev_stock_qty"),
        F.lead("seq_no").over(w6).alias("next_seq_no"),
        F.lag("seq_no").over(w6).alias("prev_seq_no"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.last("running_min_stock_qty", ignorenulls=True).over(w7).alias("last_known_running_min_stock_qty"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("next_stock_qty") >= 323, "HIGH").otherwise("LOW").alias("next_stock_qty_tier"),
    )
    .agg(
        F.count_if(F.col("category") == "Books").alias("books_events"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("running_min_stock_qty")).otherwise(0)).alias("bangkok_running_min_stock_qty"),
    )
    .orderBy(F.col("next_stock_qty_tier").desc())
)
