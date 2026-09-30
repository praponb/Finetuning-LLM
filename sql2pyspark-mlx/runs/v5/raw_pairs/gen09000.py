from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

with_latest = (
    df_orders
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price").alias("order_item_unit_price"),
    )
)

result = (
    with_latest
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .rollup("quarter_start")
    .agg(
        F.sum(F.when(F.col("status") == "RETURNED", F.col("discount"))).alias("returned_total_discount"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_rows"),
        F.sum(F.when(F.col("channel") == "web", F.col("order_item_quantity")).otherwise(0)).alias("web_order_item_quantity"),
        F.grouping("quarter_start").alias("gid"),
    )
    .filter(F.col("returned_rows") >= 1)
    .orderBy(F.col("quarter_start").desc_nulls_last())
)
