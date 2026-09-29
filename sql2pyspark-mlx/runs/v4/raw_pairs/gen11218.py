from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_first())
w2 = Window.partitionBy("store_id").orderBy(F.col("avg_quantity").desc())

ranked = (
    df_inventory
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"), F.col("p.cost"), F.col("p.brand"))
)

remaining_keys = (
    enriched
    .select("id", "product_id")
    .exceptAll(
        df_inventory
        .filter(F.col("product_id").isin([4, 5, 9]))
        .select("id", "product_id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter((F.col("last_restock") >= F.lit("2023-08-23").cast("date")) & F.col("brand").isNotNull() & (F.col("quantity") < 141))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "rank_in_group",
        "category",
        "brand",
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
    )
)

result = (
    filtered
    .groupBy("store_id", "product_id")
    .agg(
        F.round(F.sum("quantity") / F.nullif(F.count(F.when(F.col("brand") == "Umbrella", 1)), F.lit(0)), 2).alias("quantity_per_umbrella"),
        F.sum(F.when(F.col("category") == "Books", F.col("quantity")).otherwise(0)).alias("books_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        ((F.unix_timestamp(F.max("last_restock_period")) - F.unix_timestamp(F.min("last_restock_period"))) / 60).alias("span_minutes"),
    )
    .withColumn("avg_quantity_rank", F.rank().over(w2))
    .orderBy(F.col("store_id").desc())
)
