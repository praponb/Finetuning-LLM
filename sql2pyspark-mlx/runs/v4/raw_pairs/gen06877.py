from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc(), "id")
w_hist = Window.partitionBy("product_id").orderBy("revenue", "id")

scored = (
    df_store_sales
    .withColumn("revenue_bucket", F.ntile(3).over(w))
    .withColumn("next_units", F.lead("units", 1).over(w_hist))
)

enriched = (
    scored
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.region"), F.col("p.store_name"), F.col("p.opened_date"))
)

kept_keys = (
    enriched
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id") == 1)
        .select("store_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("sale_date") >= F.lit("2023-08-21").cast("date")) & F.col("region").isNotNull() & F.month("sale_date").isin([2, 8, 12]))
    .withColumn("units_filled", F.coalesce("units", F.lit(0)))
)

with_dims = (
    filtered
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.product_name"))
)

enriched_store_sales = (
    with_dims
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.opened_date").alias("store_opened_date"))
)

totals = (
    enriched_store_sales
    .groupBy(
        F.when(F.col("units") >= 31, "LARGE").otherwise("SMALL").alias("units_tier"),
    )
    .agg(
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.sum("revenue").alias("total_revenue"),
        F.percentile_approx("revenue", 0.95).alias("revenue_p95"),
    )
)

result = (
    totals
    .select("units_tier", "total_revenue", "revenue_p95")
    .orderBy(F.col("total_revenue").desc())
)
