from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").asc_nulls_last(), "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("quantity") * F.col("unit_price")))
)

deduped = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    deduped
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.customer_id").isin([26, 28, 34]))
    .select("src.*", F.col("o.order_date"), F.col("o.shipping_fee"), F.col("o.status"))
)

joined_order_items = (
    joined
    .alias("src")
    .join(df_orders.alias("b"), F.col("src.order_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.amount"), F.col("b.channel"))
)

result = (
    joined_order_items
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "line_total",
        "order_date",
        "shipping_fee",
        "status",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "PRIORITY").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
)
