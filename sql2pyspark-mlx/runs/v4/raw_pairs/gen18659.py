from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity_filled", "id")

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2024-04-16").cast("date")) & F.col("last_restock").between("2023-07-01", "2023-11-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

enriched = (
    valid
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("quantity_filled_bucket", F.ntile(10).over(w2))
)

with_dims = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.store_name"))
)

remaining_keys = (
    with_dims
    .select("store_id", "id")
    .exceptAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 4]))
        .select("store_id", "id")
    )
)

kept_rows = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    kept_rows
    .alias("a")
    .join(df_stores.alias("b"), F.col("a.store_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.sqft"), F.col("b.opened_date"), F.col("b.city"))
)

result = (
    joined
    .groupBy(
        F.when(F.col("quantity_filled") >= 78, "LARGE").otherwise("SMALL").alias("quantity_filled_tier"),
        F.when(F.col("quantity_filled_bucket") >= 6, "LARGE").otherwise("SMALL").alias("quantity_filled_bucket_tier"),
    )
    .agg(
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("quantity_per_singapore"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("quantity_filled")).otherwise(0)).alias("tokyo_quantity_filled"),
    )
)
