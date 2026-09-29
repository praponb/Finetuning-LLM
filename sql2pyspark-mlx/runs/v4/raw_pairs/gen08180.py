from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "category").orderBy("cost_int", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country", "city")

kept = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.order_id").isin([10, 19])), "left_semi")
)

enriched = (
    kept
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.city"), F.col("s.country"))
)

cleaned = (
    enriched
    .filter((F.col("launch_date") > F.lit("2023-09-03").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "city",
        "country",
        F.col("cost").cast("int").alias("cost_int"),
        F.when(F.col("stock_qty") >= 428, "High").when(F.col("stock_qty") >= 54, "Medium").otherwise("Low").alias("stock_qty_tier"),
        F.coalesce("country", F.lit("DIRECT")).alias("country_filled"),
    )
)

enriched_products = (
    cleaned
    .select(
        "*",
        F.avg("price").over(w).alias("price_avg_6"),
        F.first("brand", ignorenulls=True).over(w2).alias("first_known_brand"),
        F.stddev_samp("cost_int").over(w_hist).alias("country_stddev_samp_cost_int"),
    )
)

enriched_products_2 = (
    enriched_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.city").alias("supplier_city"))
)

stats = (
    enriched_products_2
    .rollup("country", "city")
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_rows"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.sum("cost_int") / F.nullif(F.count(F.when(F.col("supplier_city") == "London", 1)), F.lit(0)), 2).alias("cost_int_per_london"),
        F.percentile_approx("cost_int", [0.25, 0.5, 0.75]).alias("cost_int_quartiles"),
    )
    .filter(F.col("cost_int_per_london") > 0)
)

result = (
    stats
    .select(
        "country",
        "stark_rows",
        "num_countries",
        "cost_quartiles",
        "country_list",
        "cost_int_per_london",
        "cost_int_quartiles",
        F.when(F.col("country").rlike("^(UK|US)$"), "EXTENDED").when(F.col("country").rlike("^(TH|SG)$"), "CORE").otherwise("PRIORITY").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc())
)
