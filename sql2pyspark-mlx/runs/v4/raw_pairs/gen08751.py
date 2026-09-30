from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2024-03-09").cast("date")) & F.month("last_restock").isin([1, 7, 10]))
    .withColumn("last_restock_year", F.year("last_restock"))
)

kept_keys = (
    valid
    .select("product_id", "store_id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") > "2024-01-01")
        .select("product_id", "store_id")
    )
)

verified = (
    valid
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.city"))
)

metrics = (
    enriched
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.rank().over(w2).alias("quantity_rank_2"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.cost"), F.col("b.category"), F.col("b.launch_date"))
)

result = (
    with_dims
    .withColumn("week_start", F.date_trunc("week", "last_restock"))
    .withColumn("activity_month", F.date_trunc("month", "last_restock"))
    .groupingSets(
        [["week_start", "activity_month"], ["week_start"]],
        "week_start",
        "activity_month",
    )
    .agg(
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.bit_or("quantity").alias("quantity_bit_or_2"),
        F.grouping_id().alias("grouping_id"),
    )
)
