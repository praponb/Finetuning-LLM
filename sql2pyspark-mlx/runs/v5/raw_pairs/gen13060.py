from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("stock_qty").asc_nulls_last(), "id")

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-02").cast("date"))
    .select("*", F.greatest("revenue", "units").alias("greatest_revenue_units"))
)

enriched = (
    base
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .filter(F.length(F.col("b.store_name")) < 6)
    .select("s.id", "s.store_id", "s.product_id", "s.units", F.col("b.store_name"))
)

valid = (
    enriched
    .filter(F.col("units") < 29)
    .select(
        "id",
        "product_id",
        "units",
        "store_name",
        F.round("units", 1).alias("units_rounded"),
        F.col("units").cast("double").alias("units_num"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.units",
        "s.store_name",
        "s.units_rounded",
        "s.units_num",
        F.col("d.stock_qty"),
        F.col("d.brand"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "product_id", "units", "units_rounded", "units_num")
)

cleaned = (
    deduped
    .filter(F.col("units_num") > 42)
    .select(
        "id",
        "product_id",
        "units",
        "units_rounded",
        F.col("units").cast("double").alias("units_str"),
        F.round((F.col("units") * 0.9), 2).alias("units_adjusted"),
    )
)

result = (
    cleaned
    .withColumn("units_rounded_tier", F.when(F.col("units_rounded") >= 83, "HIGH").otherwise("LOW"))
    .rollup("units_rounded_tier")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("units_adjusted", 0.95).alias("units_adjusted_p95"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("units_adjusted_p95") > 0)
    .orderBy(F.col("distinct_ids_2").desc_nulls_first())
)
