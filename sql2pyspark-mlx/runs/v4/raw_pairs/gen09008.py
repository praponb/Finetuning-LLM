from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.partitionBy("store_id").orderBy("revenue", "id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-04-12").cast("date"))
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

scored = (
    valid
    .select(
        "*",
        F.last("units", ignorenulls=True).over(w).alias("last_known_units"),
        F.stddev_samp("units").over(w2).alias("store_id_stddev_samp_units"),
        F.lag("units", 1).over(w3).alias("prev_units"),
    )
)

kept_keys = (
    scored
    .select("store_id", "id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue") > 3200)
        .select("store_id", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("sale_date") >= F.lit("2023-06-11").cast("date"))
    .select("*", F.col("revenue_per_unit").cast("int").alias("revenue_per_unit_int"))
)

enriched = (
    cleaned
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.price"), F.col("b.cost"), F.col("b.launch_date"))
)

valid_store_sales = (
    enriched
    .filter(F.col("price") >= 860)
    .select(
        "store_id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "store_id_stddev_samp_units",
        "prev_units",
        "revenue_per_unit_int",
        "cost",
        F.round("units", 0).alias("units_rounded"),
    )
)

result = (
    valid_store_sales
    .select(
        "product_id",
        "revenue",
        "store_id_stddev_samp_units",
        "revenue_per_unit_int",
        "units_rounded",
    )
    .orderBy("units_rounded")
)
