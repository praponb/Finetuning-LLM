from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.partitionBy("order_id").orderBy(F.col("line_total").desc(), "id")
w3 = Window.orderBy("unit_price_dense_rank", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy(F.col("last3_sum_line_total_2").asc_nulls_last())
w5 = Window.orderBy(F.col("last3_avg_line_total_2").desc())
w_part = Window.orderBy("line_total_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_order_items
    .filter(F.col("unit_price") < 180)
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("quantity") * F.col("unit_price")).alias("line_total"),
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total_2"),
    )
)

metrics = (
    filtered
    .filter(F.col("quantity") <= 3)
    .select(
        "id",
        "quantity",
        "line_total_2",
        F.dense_rank().over(w).alias("unit_price_dense_rank"),
        F.ntile(4).over(w2).alias("line_total_bucket"),
    )
)

metrics_order_items = (
    metrics
    .select(
        "id",
        "quantity",
        "line_total_bucket",
        F.avg("line_total_2").over(w3).alias("last3_avg_line_total_2"),
        F.min("line_total_2").over(w3).alias("last3_min_line_total_2"),
        F.sum("line_total_2").over(w3).alias("last3_sum_line_total_2"),
        F.max("line_total_2").over(w3).alias("last3_max_line_total_2"),
    )
)

scored = (
    metrics_order_items
    .withColumn("last3_sum_line_total_2_dense_rank", F.dense_rank().over(w4))
    .withColumn("last3_avg_line_total_2_cume_dist", F.cume_dist().over(w5))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w_part))
)

valid = (
    scored
    .filter(F.col("last3_avg_line_total_2").between(24, 43))
    .select(
        "id",
        "quantity",
        "line_total_bucket",
        "last3_avg_line_total_2",
        "last3_min_line_total_2",
        "last3_sum_line_total_2",
        "last3_max_line_total_2",
        "last3_sum_line_total_2_dense_rank",
        "last3_avg_line_total_2_cume_dist",
        "last_known_quantity",
        F.when(F.col("last_known_quantity") >= 9, "High").when(F.col("last_known_quantity") >= 4, "Medium").otherwise("Low").alias("last_known_quantity_level"),
    )
)

result = (
    valid
    .select(
        "id",
        "quantity",
        "line_total_bucket",
        "last3_avg_line_total_2",
        "last3_min_line_total_2",
        "last3_sum_line_total_2",
        "last3_sum_line_total_2_dense_rank",
        "last_known_quantity",
    )
    .orderBy(F.col("id").desc())
)
