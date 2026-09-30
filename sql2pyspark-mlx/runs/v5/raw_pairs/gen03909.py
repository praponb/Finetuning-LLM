from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy(F.col("cost").desc_nulls_first())
w5 = Window.partitionBy("store_id").orderBy("running_quantity", "id")
w_ordered = Window.partitionBy("product_id").orderBy("cost_per_first_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

joined = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.cost"))
)

with_windows = (
    joined
    .select(
        "id",
        "store_id",
        "product_id",
        "cost",
        F.first("cost", ignorenulls=True).over(w2).alias("first_known_cost"),
        F.sum("quantity").over(w3).alias("running_quantity"),
    )
)

base = (
    with_windows
    .filter(F.col("first_known_cost").between(360, 510))
    .select(
        "*",
        F.round((F.col("cost") / F.col("first_known_cost")), 2).alias("cost_per_first_known_cost"),
        F.greatest("running_quantity", "first_known_cost").alias("greatest_running_quantity_first_known_cost"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .filter(F.col("cost").between(240, 670))
    .withColumn("running_quantity_bucket", F.ntile(4).over(w5))
    .withColumn("running_cost_per_first_known_cost", F.sum("cost_per_first_known_cost").over(w_ordered))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.first_known_cost",
        "s.running_quantity",
        "s.cost_per_first_known_cost",
        "s.running_quantity_bucket",
        F.col("b.city"),
    )
)

result = (
    with_dims
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("s.region").alias("region"),
        F.col("s.city").alias("store_city"),
        F.col("a.city").alias("inventory_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.running_quantity_bucket")).alias("min_running_quantity_bucket"),
        F.max(F.col("a.cost_per_first_known_cost")).alias("max_cost_per_first_known_cost"),
    )
    .orderBy(F.col("num_rows").desc_nulls_last())
)
