from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w_ordered = Window.partitionBy("product_id")
w3 = Window.orderBy(F.col("quantity").desc(), "id")
w_grp = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_order_items
    .filter(F.col("order_id") == 34)
    .select(
        "id",
        "product_id",
        "quantity",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.product_name"))
)

latest = (
    with_dims
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

cleaned = (
    latest
    .filter(F.col("product_id").isNotNull() & (F.col("rn") <= 1))
    .select("*", F.initcap(F.trim("product_name")).alias("product_name_title"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.min("quantity").over(w_ordered).alias("product_id_min_quantity"),
        F.row_number().over(w3).alias("event_seq"),
        F.last("quantity", ignorenulls=True).over(w_grp).alias("last_known_quantity"),
        F.last("line_total", ignorenulls=True).over(w_grp).alias("last_known_line_total"),
    )
)

valid_order_items = (
    metrics
    .filter(F.col("quantity") < 6)
    .withColumn("product_name_normalized", F.upper(F.trim("product_name")))
    .withColumn("product_name_prefix", F.substring("product_name", 1, 4))
)

result = (
    valid_order_items
    .groupBy("product_id")
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.round(F.avg("product_id_min_quantity"), 2).alias("avg_product_id_min_quantity"),
    )
    .filter(F.col("avg_product_id_min_quantity") < 0)
)
