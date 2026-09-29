from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("order_id", "product_id").orderBy(F.col("first_known_line_total").asc_nulls_first(), "id")
w4 = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", (F.col("unit_price") - F.col("quantity")).alias("line_total"))
)

enriched = (
    valid
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.discount"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("unit_price").over(w).alias("order_id_sum_unit_price"),
        F.first("line_total", ignorenulls=True).over(w2).alias("first_known_line_total"),
    )
)

ranked = (
    with_windows
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

deduped = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

enriched_order_items = (
    deduped
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.product_name"))
)

joined = (
    enriched_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.category"), F.col("d.brand"), F.col("d.cost"))
)

summary = (
    joined
    .withColumn("unit_price_tier", F.when(F.col("unit_price") >= 320, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["product_id", "unit_price_tier"], ["product_id"]],
        "product_id",
        "unit_price_tier",
    )
    .agg(
        F.round(F.sum("order_id_sum_unit_price") / F.nullif(F.count(F.when(F.col("brand") == "Initech", 1)), F.lit(0)), 2).alias("order_id_sum_unit_price_per_initech"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("order_id_sum_unit_price")).otherwise(0)).alias("stark_order_id_sum_unit_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
)

result = (
    summary
    .select(
        "unit_price_tier",
        "order_id_sum_unit_price_per_initech",
        "num_categories",
        "home_events",
        "category_list",
    )
    .orderBy(F.col("num_categories").asc_nulls_last())
)
