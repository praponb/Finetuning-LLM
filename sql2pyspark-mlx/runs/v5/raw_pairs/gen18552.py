from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("line_total").desc_nulls_last(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").isNull())
    .select(
        "id",
        "order_id",
        "quantity",
        F.round((F.col("quantity") / F.col("unit_price")), 2).alias("line_total"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "order_id", "quantity", "line_total")
)

base_order_items = (
    deduped
    .filter(F.col("order_id").isin([2, 15, 18, 32]))
    .select("*", F.coalesce("line_total", F.lit(0)).alias("line_total_filled"))
)

base_order_items_2 = (
    base_order_items
    .filter(F.col("quantity") < 3)
    .select(
        "id",
        "order_id",
        "quantity",
        "line_total_filled",
        F.col("line_total").cast("int").alias("line_total_int"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)

result = (
    base_order_items_2
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "inner")
    .cube(F.col("d.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("s.line_total_int")), 2).alias("total_line_total_int"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("rows_total").desc_nulls_last())
)
