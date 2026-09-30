from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("brand").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

enriched = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "launch_date",
        "supplier_id",
        F.lead("price").over(w_running).alias("next_price"),
        F.lag("cost").over(w_running).alias("prev_cost"),
        F.row_number().over(w_running).alias("event_seq"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

with_last_products = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.event_seq",
        "s.cost_share",
        F.col("c.quantity"),
    )
)

kept = (
    with_last_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([3, 4, 8])), "left_semi")
)

result = (
    kept
    .groupBy("brand")
    .agg(
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("event_seq_per_electronics"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
    )
    .orderBy(F.col("brand").desc())
)
