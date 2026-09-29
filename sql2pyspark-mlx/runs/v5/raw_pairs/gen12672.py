from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("order_id", "product_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_rounded", F.round("unit_price", 0))
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

scored = (
    base
    .select(
        "id",
        "product_id",
        "quantity",
        "line_total",
        F.avg("unit_price").over(w_grp).alias("running_avg_unit_price"),
        F.max("unit_price").over(w_grp).alias("running_max_unit_price"),
        F.sum("unit_price").over(w_grp).alias("running_sum_unit_price"),
    )
)

cleaned = (
    scored
    .filter(F.col("running_max_unit_price") <= 280)
    .select(
        "id",
        "quantity",
        "line_total",
        "running_avg_unit_price",
        "running_max_unit_price",
        "running_sum_unit_price",
        F.least("running_sum_unit_price", "running_avg_unit_price").alias("least_running_sum_unit_price_running_avg_unit_price"),
    )
)

valid = (
    cleaned
    .filter(F.col("quantity").between(6, 7))
    .withColumn("running_sum_unit_price_rounded", F.round("running_sum_unit_price", 1))
)

filtered = (
    valid
    .filter(F.col("least_running_sum_unit_price_running_avg_unit_price").isNotNull())
    .select(
        "id",
        "quantity",
        "line_total",
        "running_avg_unit_price",
        "running_max_unit_price",
        "running_sum_unit_price",
        "least_running_sum_unit_price_running_avg_unit_price",
        "running_sum_unit_price_rounded",
        (F.col("running_sum_unit_price_rounded") + F.col("running_avg_unit_price")).alias("running_sum_unit_price_rounded_plus_running_avg_unit_price"),
    )
)

result = (
    filtered
    .select(
        "id",
        "quantity",
        "running_avg_unit_price",
        "running_max_unit_price",
        "running_sum_unit_price",
        "least_running_sum_unit_price_running_avg_unit_price",
        "running_sum_unit_price_rounded",
        "running_sum_unit_price_rounded_plus_running_avg_unit_price",
    )
    .orderBy(F.col("quantity").desc())
)
