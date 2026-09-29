from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("order_id")
w3 = Window.orderBy("line_total", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-6, -1)
w6 = Window.partitionBy("order_id").orderBy(F.col("line_total_avg_6_prior").desc_nulls_first(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("unit_price") * F.col("quantity")))
)

enriched = (
    base
    .select(
        "*",
        F.round((F.col("unit_price") / F.sum("unit_price").over(w)), 4).alias("unit_price_share"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share_2"),
        F.sum("line_total").over(w3).alias("running_line_total"),
        F.row_number().over(w4).alias("row_seq"),
        F.avg("line_total").over(w5).alias("line_total_avg_6_prior"),
    )
)

valid = (
    enriched
    .filter(F.col("row_seq") != 7)
    .withColumn("greatest_unit_price_share_line_total", F.greatest("unit_price_share", "line_total"))
)

joined = (
    valid
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty").between(172, 254))
    .select("a.*", F.col("p.stock_qty"), F.col("p.cost"))
)

latest = (
    joined
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "line_total",
        "unit_price_share",
        "row_seq",
        "line_total_avg_6_prior",
        "stock_qty",
        "cost",
    )
)

result = (
    latest
    .groupBy(
        F.when(F.col("quantity") >= 9, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.when(F.col("cost") >= 690, "HIGH").otherwise("LOW").alias("cost_tier"),
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.countDistinct("order_id").alias("distinct_order_ids_2"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .orderBy(F.col("cost_tier").asc_nulls_last())
)
