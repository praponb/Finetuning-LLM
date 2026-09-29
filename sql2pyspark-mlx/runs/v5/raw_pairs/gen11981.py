from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "order_id").orderBy(F.col("unit_price_rounded").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("row_num")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("quantity").between(4, 6))
    .select(
        "*",
        F.round("unit_price", 2).alias("unit_price_rounded"),
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .filter(F.col("b.launch_date") > "2023-12-01")
    .select("a.*", F.col("b.cost"), F.col("b.brand"), F.col("b.product_name"))
)

latest = (
    joined
    .withColumn("latest_rank", F.rank().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    latest
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["status", "channel"], ["status"], []],
        F.col("o.status").alias("status"),
        F.col("o.channel").alias("channel"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("src.line_total")).alias("min_line_total"),
    )
)
