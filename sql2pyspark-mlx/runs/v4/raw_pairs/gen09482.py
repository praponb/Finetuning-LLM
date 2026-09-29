from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("num_brands").desc())

valid = (
    df_order_items
    .filter(F.col("unit_price").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.product_name"), F.col("b.brand"), F.col("b.cost"))
)

enriched_order_items = (
    enriched
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.product_name").alias("product_product_name"),
        F.col("d.launch_date"),
    )
)

enriched_order_items_2 = (
    enriched_order_items
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.discount"))
)

stats = (
    enriched_order_items_2
    .groupBy(
        "order_id",
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
        "brand",
    )
    .agg(
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.count_if(F.col("brand") == "Initech").alias("initech_events"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .withColumn("num_brands_rank", F.rank().over(w))
)

result = (
    stats
    .select(
        "order_id",
        "week_start",
        "brand",
        "distinct_order_ids",
        "num_brands",
        "cost_quartiles",
    )
    .orderBy("brand")
)
