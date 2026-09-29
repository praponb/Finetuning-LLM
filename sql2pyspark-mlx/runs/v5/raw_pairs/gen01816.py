from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").asc_nulls_first())

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select("*", F.month("launch_date").alias("launch_date_month"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id") == 31), "left_anti")
)

kept_keys = (
    eligible
    .select("brand", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("cost") >= 320)
        .select("brand", "supplier_id")
    )
)

reconciled = (
    eligible
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

scored = reconciled.select("*", F.rank().over(w).alias("cost_rank"))

joined = (
    scored
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.price",
        "a.tags",
        "a.supplier_id",
        "a.launch_date_month",
        F.col("b.country"),
        F.col("b.rating"),
        F.col("b.city"),
    )
)

not_flagged = (
    joined
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([14, 19, 27, 35])), "left_anti")
)

enriched = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "left")
    .filter(F.col("p.rating") >= 2.1)
    .select("s.*", F.col("p.supplier_name"))
)

result = (
    enriched
    .withColumn("launch_date_month_tier", F.when(F.col("launch_date_month") >= 64, "LARGE").otherwise("SMALL"))
    .rollup("launch_date_month_tier")
    .agg(
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.count("*").alias("num_rows"),
        F.round(F.sum("launch_date_month") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("launch_date_month_per_us"),
        F.count_if(F.col("country") == "TH").alias("th_events"),
        F.grouping_id().alias("grouping_level"),
    )
)
