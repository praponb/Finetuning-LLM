from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units_rounded").asc_nulls_first(), "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-09-30").cast("date")) & (F.col("units") == 37))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.round("units", 0).alias("units_rounded"),
    )
)

joined = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.id", "e.product_id", "e.units_rounded", F.col("p.launch_date"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

joined_store_sales = (
    ranked
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.product_name").like("%Backpack%"))
    .select(
        "s.id",
        "s.product_id",
        "s.units_rounded",
        "s.launch_date",
        F.col("d.brand"),
    )
)

enriched = (
    joined_store_sales
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.cost"))
)

result = (
    enriched
    .groupBy("product_id", "brand")
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("event_count"),
    )
)
