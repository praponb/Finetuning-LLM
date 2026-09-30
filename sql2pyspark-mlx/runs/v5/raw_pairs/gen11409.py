from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("rn_whole", "id")
w5 = Window.partitionBy("category").orderBy("rn_whole", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") + F.col("quantity")).alias("line_total"),
    )
)

ranked = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "product_id", "rn")
)

cleaned = (
    deduped
    .filter(F.col("product_id").isNull())
    .select("*", F.round("rn", 0).alias("rn_rounded"), F.ceil("rn").alias("rn_whole"))
)

enriched = (
    cleaned
    .withColumn("running_rn_whole", F.sum("rn_whole").over(w3))
    .withColumn("next_rn_whole", F.lead("rn_whole", 2).over(w4))
)

enriched_order_items = (
    enriched
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.category"), F.col("d.price"))
)

with_windows = (
    enriched_order_items
    .select(
        "*",
        F.lead("rn_whole").over(w5).alias("next_rn_whole_2"),
        F.lag("running_rn_whole").over(w5).alias("prev_running_rn_whole"),
        F.lead("running_rn_whole").over(w5).alias("next_running_rn_whole"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

pivoted = (
    with_windows
    .groupBy("product_id")
    .pivot("category", ["Books", "Grocery"])
    .agg(
        F.sum("next_running_rn_whole").alias("value_sum"),
        F.count(F.lit(1)).alias("n"),
    )
)

result = (
    pivoted
    .select(
        "product_id",
        "Books_value_sum",
        "Books_n",
        "Grocery_value_sum",
        "Grocery_n",
    )
    .orderBy(F.col("Books_n").desc_nulls_first())
)
