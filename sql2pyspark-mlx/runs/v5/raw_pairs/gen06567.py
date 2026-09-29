from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w_part = Window.orderBy("rating", "id")
w_recent = Window.partitionBy("city", "country")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w_ordered = Window.partitionBy("country").orderBy("stock_qty", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("stock_qty").desc())
w7 = Window.partitionBy("city").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city").orderBy("stock_qty", "id")
w9 = Window.partitionBy("country").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_suppliers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .withColumn("prev_rating", F.lag("rating", 2).over(w_part))
    .withColumn("city_max_rating", F.max("rating").over(w_recent))
)

with_latest = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.country", "s.city", F.col("c.stock_qty"))
)

enriched_suppliers = (
    with_latest
    .select(
        "id",
        "country",
        F.avg("stock_qty").over(w_ordered).alias("stock_qty_avg_6"),
        F.dense_rank().over(w6).alias("stock_qty_dense_rank"),
        F.last("stock_qty", ignorenulls=True).over(w7).alias("last_known_stock_qty"),
        F.lag("stock_qty", 2).over(w8).alias("prev_stock_qty"),
        F.sum("stock_qty").over(w9).alias("cumulative_stock_qty"),
    )
)

with_last = (
    enriched_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.stock_qty_avg_6",
        "s.prev_stock_qty",
        F.col("c.cost"),
        F.col("c.launch_date"),
    )
)

stats = (
    with_last
    .groupBy(
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
        F.when(F.col("stock_qty_avg_6") >= 411, "HIGH").otherwise("LOW").alias("stock_qty_avg_6_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.percentile_approx("cost", 0.95).alias("cost_p95"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("stock_qty_avg_6"), 2).alias("avg_stock_qty_avg_6"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("event_count_2"),
    )
)

result = (
    stats
    .select(
        "activity_month",
        "stock_qty_avg_6_tier",
        "event_count",
        "cost_p95",
        "avg_cost",
        "avg_stock_qty_avg_6",
        "num_rows",
    )
    .orderBy(F.col("avg_stock_qty_avg_6").desc_nulls_last())
)
