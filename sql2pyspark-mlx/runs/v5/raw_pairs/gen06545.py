from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("product_stock_qty_p50").desc())

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

with_last = (
    valid
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.city_filled",
        F.col("c.cost"),
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

remaining_keys = (
    with_last
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("rating") <= 2.6)
        .select("id", "city")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

matched_keys = (
    reconciled
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("country") == "TH")
        .select("id", "city")
    )
)

kept_rows = (
    reconciled
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .groupBy(
        "city",
        F.when(F.col("cost") >= 580, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.percentile_approx("product_stock_qty", 0.5).alias("product_stock_qty_p50"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("product_stock_qty")).otherwise(0)).alias("tokyo_product_stock_qty"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .filter(F.col("tokyo_product_stock_qty") < 1000000)
    .withColumn("product_stock_qty_p50_rank", F.rank().over(w2))
)

result = (
    daily
    .select("city", "cost_tier", "product_stock_qty_p50", "tokyo_product_stock_qty")
    .orderBy(F.col("city").desc_nulls_last())
)
