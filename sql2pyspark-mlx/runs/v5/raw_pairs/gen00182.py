from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("line_total").desc(), "id")
w_running = Window.partitionBy("order_id", "product_id").orderBy("line_total", "id")
w_running_2 = Window.partitionBy("order_id").orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy("quantity_num_2", "id")
w6 = Window.partitionBy("order_id").orderBy(F.col("quantity_num_2").desc(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

filtered = (
    base
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(5, 7))
    .withColumn("line_total", (F.col("unit_price") + F.col("quantity")))
    .withColumn("quantity_num_2", F.col("quantity").cast("double"))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    latest
    .filter(F.col("quantity_num_2").between(45, 77))
    .select(
        "id",
        "order_id",
        "quantity",
        "quantity_num",
        "line_total",
        "quantity_num_2",
        F.row_number().over(w_running).alias("seq_no"),
        F.first("unit_price", ignorenulls=True).over(w_running_2).alias("first_known_unit_price"),
        F.sum("unit_price").over(w_hist).alias("cumulative_unit_price"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "left")
    .select("a.*", F.col("o.order_date"))
)

latest_order_items = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_order_items_2 = (
    latest_order_items
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    latest_order_items_2
    .select(
        "id",
        "order_id",
        "quantity",
        "line_total",
        "quantity_num_2",
        "first_known_unit_price",
        "order_date",
        "row_num",
    )
    .orderBy(F.col("order_date").desc())
)
