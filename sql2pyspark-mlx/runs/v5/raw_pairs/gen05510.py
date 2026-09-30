from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "order_id")
w3 = Window.orderBy(F.col("running_min_quantity").desc_nulls_first())
w4 = Window.partitionBy("order_id").orderBy(F.col("running_min_quantity").desc())

metrics = (
    df_order_items
    .select(
        "id",
        "order_id",
        "product_id",
        F.min("quantity").over(w_running).alias("running_min_quantity"),
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.avg("quantity").over(w_running).alias("running_avg_quantity"),
        F.count("quantity").over(w_running).alias("running_count_quantity"),
    )
)

remaining_keys = (
    metrics
    .select("id", "product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id").isin([8, 19, 28, 38]))
        .select("id", "product_id")
    )
)

kept_rows = (
    metrics
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

metrics_order_items = (
    kept_rows
    .filter(F.col("product_id") == 11)
    .withColumn("product_id_stddev_samp_running_count_quantity", F.stddev_samp("running_count_quantity").over(w2))
    .withColumn("running_min_quantity_cume_dist", F.cume_dist().over(w3))
    .withColumn("running_min_quantity_percent_rank", F.percent_rank().over(w4))
)

kept_keys = (
    metrics_order_items
    .select("product_id", "id")
    .intersect(
        df_order_items
        .filter(F.col("quantity") >= 3)
        .select("product_id", "id")
    )
)

verified = (
    metrics_order_items
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "order_id",
        "running_min_quantity_percent_rank",
        "running_sum_quantity",
        "running_min_quantity",
    )
    .unpivot(
        ids=["order_id"],
        values=["running_min_quantity_percent_rank", "running_sum_quantity", "running_min_quantity"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
