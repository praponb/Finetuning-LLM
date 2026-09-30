from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy("quantity", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("product_id", "order_id").orderBy("order_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("discount", "id")
w5 = Window.orderBy("quantity_avg_3_prior", "id")
w6 = Window.orderBy(F.col("quantity").desc(), "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("next_line_total").desc(), "id")
w8 = Window.partitionBy("order_id").orderBy("quantity", "id")
w9 = Window.orderBy("quantity_avg_3_prior_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

deduped = (
    df_order_items
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    deduped
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("unit_price") + F.col("quantity")).alias("line_total"))
)

joined = (
    valid
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.order_date"), F.col("p.discount"), F.col("p.amount"))
)

metrics = (
    joined
    .select(
        "*",
        F.avg("quantity").over(w2).alias("quantity_avg_3_prior"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
        F.lead("line_total", 1, 0).over(w4).alias("next_line_total"),
    )
)

with_windows = (
    metrics
    .select(
        "*",
        F.ntile(4).over(w5).alias("quantity_avg_3_prior_bucket"),
        F.ntile(4).over(w6).alias("quantity_bucket"),
        F.row_number().over(w7).alias("event_seq"),
        F.row_number().over(w8).alias("row_seq"),
    )
)

enriched = (
    with_windows
    .withColumn("first_known_quantity_avg_3_prior", F.first("quantity_avg_3_prior", ignorenulls=True).over(w9))
    .withColumn("first_known_quantity_avg_3_prior_2", F.first("quantity_avg_3_prior", ignorenulls=True).over(w10))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "product_id",
        "line_total",
        "discount",
        "amount",
        "last_known_quantity",
        "next_line_total",
        "quantity_avg_3_prior_bucket",
    )
    .orderBy(F.col("order_id").desc())
)
