from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("cost").desc())
w2 = Window.partitionBy("category", "brand")
w3 = Window.partitionBy("brand", "category")

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") > 280), "left_anti")
)

matched_keys = (
    kept
    .select("id", "brand")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isNull())
        .select("id", "brand")
    )
)

reconciled = (
    kept
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.brand") == F.col("k.brand")), "inner")
    .select("s.*")
)

enriched = (
    reconciled
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.rating"), F.col("p2.city"), F.col("p2.supplier_name"))
)

base = (
    enriched
    .filter((F.col("launch_date") > F.lit("2023-11-14").cast("date")) & (F.size("tags") >= 1))
    .withColumn("rating_plus_price", (F.col("rating") + F.col("price")))
    .withColumn("category_prefix", F.substring("category", 1, 3))
)

with_windows = (
    base
    .withColumn("cost_dense_rank", F.dense_rank().over(w))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("price_share", F.round((F.col("price") / F.sum("price").over(w3)), 4))
)

kept_products = (
    with_windows
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") > 330), "left_anti")
)

enriched_products = (
    kept_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.country"))
)

totals = (
    enriched_products
    .groupBy(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
    )
    .agg(
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.percentile_approx("rating_plus_price", 0.95).alias("rating_plus_price_p95"),
    )
    .filter(F.col("fashion_events") < 1)
)

result = (
    totals
    .select(
        "quarter_start",
        "activity_month",
        "fashion_events",
        "rating_plus_price_p95",
    )
)
