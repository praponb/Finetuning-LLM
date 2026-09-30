from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("last_restock").asc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-10-25").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

joined = (
    deduped
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.launch_date"), F.col("d.brand"))
)

with_dims = (
    joined
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.store_name"))
)

kept_keys = (
    with_dims
    .select("id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([1, 3, 4, 5]))
        .select("id", "product_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("quantity_filled_tier", F.when(F.col("quantity_filled") >= 78, "PREMIUM").otherwise("STANDARD"))
    .rollup("store_id", "quantity_filled_tier")
    .agg(
        F.avg(F.when(F.col("brand") == "Acme", F.col("quantity_filled"))).alias("acme_avg_quantity_filled"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.grouping("store_id").alias("grouping_id"),
    )
    .orderBy(F.col("quantity_filled_tier").desc_nulls_first())
)
