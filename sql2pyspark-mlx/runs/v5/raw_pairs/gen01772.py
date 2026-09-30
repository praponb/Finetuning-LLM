from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("cost").desc())

enriched = (
    df_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .filter(F.array_contains(F.col("p.tags"), "clearance"))
    .select(
        "src.id",
        "src.product_id",
        "src.last_restock",
        F.col("p.price"),
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)

kept_keys = (
    enriched
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 2)
        .select("product_id", "id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.sum("price").over(w).alias("product_id_sum_price"),
        F.min("cost").over(w).alias("product_id_min_cost"),
        F.dense_rank().over(w2).alias("cost_dense_rank"),
    )
)

cleaned = (
    metrics
    .filter(F.col("launch_date") >= "2023-09-01")
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
)

with_dims = (
    cleaned
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.product_id_sum_price",
        "a.cost_dense_rank",
        "a.launch_date_month_end",
        F.col("d.cost").alias("product_cost"),
    )
)

result = (
    with_dims
    .select(
        "id",
        "cost_dense_rank",
        "launch_date_month_end",
        "product_cost",
        F.round((F.col("product_cost") / F.sum("product_cost").over(w)), 4).alias("product_cost_share"),
    )
)
