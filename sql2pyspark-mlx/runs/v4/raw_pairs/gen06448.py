from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered_2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("store_id", "product_id")
w5 = Window.partitionBy("product_id", "store_id")
w6 = Window.orderBy("last_known_days_since_sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("units") < 28)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.sum("days_since_sale_date").over(w_ordered).alias("cumulative_days_since_sale_date"),
        F.sum("days_since_sale_date").over(w_grp).alias("cumulative_days_since_sale_date_2"),
        F.last("days_since_sale_date", ignorenulls=True).over(w_ordered_2).alias("last_known_days_since_sale_date"),
        F.min("days_since_sale_date").over(w_running).alias("store_id_min_days_since_sale_date"),
    )
)

with_windows = (
    scored
    .withColumn("store_id_min_cumulative_days_since_sale_date", F.min("cumulative_days_since_sale_date").over(w_running))
    .withColumn("product_id_max_last_known_days_since_sale_date", F.max("last_known_days_since_sale_date").over(w5))
    .withColumn("last_known_cumulative_days_since_sale_date_2", F.last("cumulative_days_since_sale_date_2", ignorenulls=True).over(w6))
    .withColumn("cumulative_cumulative_days_since_sale_date", F.sum("cumulative_days_since_sale_date").over(w6))
)

kept_keys = (
    with_windows
    .select("product_id", "id")
    .intersectAll(
        df_store_sales
        .filter(F.col("revenue").between(2000, 4000))
        .select("product_id", "id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

grouped = (
    kept_rows
    .withColumn("quarter_start", F.date_trunc("QUARTER", "sale_date"))
    .groupingSets(
        [["quarter_start", "store_id"], ["quarter_start"]],
        "quarter_start",
        "store_id",
    )
    .agg(
        F.percentile_approx("cumulative_days_since_sale_date_2", 0.5).alias("cumulative_days_since_sale_date_2_p50"),
        F.round(F.avg("cumulative_cumulative_days_since_sale_date"), 2).alias("avg_cumulative_cumulative_days_since_sale_date"),
        F.round(F.avg("product_id_max_last_known_days_since_sale_date"), 2).alias("avg_product_id_max_last_known_days_since_sale_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.grouping("quarter_start").alias("gid"),
    )
    .filter(F.col("distinct_ids") >= 1)
)

result = (
    grouped
    .select(
        "quarter_start",
        "store_id",
        "cumulative_days_since_sale_date_2_p50",
        "avg_cumulative_cumulative_days_since_sale_date",
        "avg_product_id_max_last_known_days_since_sale_date",
        "distinct_ids",
    )
    .orderBy(
        F.col("avg_product_id_max_last_known_days_since_sale_date").desc(),
        F.col("distinct_ids").desc(),
    )
)
