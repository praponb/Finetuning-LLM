from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_minus_7d").asc_nulls_first(), "id")

filtered = (
    df_inventory
    .filter(F.col("store_id") == 1)
    .select(
        "*",
        F.date_sub("last_restock", 7).alias("last_restock_minus_7d"),
        F.ceil("quantity").alias("quantity_whole"),
    )
)

joined = (
    filtered
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.brand"), F.col("p.price"), F.col("p.category"))
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

stats = (
    deduped
    .groupBy(
        F.when(F.col("quantity") >= 194, "LARGE").otherwise("SMALL").alias("quantity_tier"),
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
        F.date_trunc("QUARTER", "last_restock").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
        F.percentile_approx("quantity_whole", [0.25, 0.5, 0.75]).alias("quantity_whole_quartiles"),
    )
    .filter(F.col("event_count") < 1)
)

result = stats.select("quantity_tier", "year_start", "quarter_start", "event_count")
