from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_id").orderBy("rating")
w3 = Window.partitionBy("city").orderBy(F.col("stock_qty").asc_nulls_last())
w4 = Window.partitionBy("cost_share_tier").orderBy(F.col("avg_cost").desc())

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-09-02").cast("date")) & F.col("brand").isNotNull())
    .withColumn("greatest_cost_stock_qty", F.greatest("cost", "stock_qty"))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("UNKNOWN")))
)

matched_keys = (
    base
    .select("brand", "id")
    .subtract(
        df_products
        .filter(F.col("price") > 390)
        .select("brand", "id")
    )
)

verified = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.stock_qty",
        "s.launch_date",
        "s.tags",
        "s.supplier_id",
        "s.brand_clean",
        F.col("p.rating"),
        F.col("p.country"),
        F.col("p.city"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "country",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.percent_rank().over(w3).alias("stock_qty_percent_rank"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([2, 9, 10])), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("cost_share") >= 0.67, "PREMIUM").otherwise("STANDARD").alias("cost_share_tier"),
        "country",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("price_share", 0.95).alias("price_share_p95"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.avg("stock_qty_percent_rank").alias("avg_stock_qty_percent_rank"),
    )
    .withColumn("avg_cost_percent_rank", F.percent_rank().over(w4))
    .orderBy("price_share_p95")
)
