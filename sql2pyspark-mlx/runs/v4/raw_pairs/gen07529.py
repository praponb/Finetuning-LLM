from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost_filled").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("cost").between(250, 390))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.launch_date")), "left_semi")
)

deduped = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_last = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price").alias("order_item_unit_price"))
)

stats = (
    with_last
    .groupBy("category")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("price")).otherwise(0)).alias("fashion_price"),
    )
)

result = (
    stats
    .select(
        "category",
        "distinct_ids",
        "num_rows",
        "home_rows",
        "fashion_price",
        F.when(F.col("category").rlike("^(Fashion|Home)$"), "EXTENDED").when(F.col("category").rlike("^(Electronics|Sports)$"), "CORE").otherwise("PRIORITY").alias("category_zone"),
    )
    .orderBy(F.col("home_rows").desc_nulls_first())
)
