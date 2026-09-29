from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity_rounded").asc_nulls_last(), "id")
w3 = Window.partitionBy("brand", "store_id").orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2024-04-29").cast("date"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
    .withColumn("quantity_rounded", F.round("quantity", 1))
)

joined = (
    cleaned
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.opened_date"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.category"), F.col("p.brand"))
)

ranked = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_rounded",
        "brand",
        "dedup_rank",
        "rank_in_group",
        F.when(F.col("brand").rlike("^(Stark|Acme)$"), "OTHER_ZONE").otherwise("EXTENDED").alias("brand_zone"),
    )
    .orderBy(F.col("rank_in_group").desc(), F.col("last_restock").desc())
)
