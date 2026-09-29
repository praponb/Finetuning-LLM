from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "region").orderBy(F.col("quantity_filled").desc_nulls_last(), "id")
w2 = Window.partitionBy("region").orderBy("quantity_filled", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-06-24").cast("date"))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM-dd").alias("last_restock_label"),
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .select("s.*", F.col("p2.city"), F.col("p2.region"))
)

ranked = (
    enriched
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "last_restock", "quantity_filled", "city", "region")
)

ranked_inventory = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

totals = (
    ranked_inventory
    .groupBy(
        F.when(F.col("quantity_filled") >= 65, "PREMIUM").otherwise("STANDARD").alias("quantity_filled_tier"),
        F.date_trunc("MONTH", "last_restock").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("quantity_filled", [0.25, 0.5, 0.75]).alias("quantity_filled_quartiles"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.round(F.avg("quantity_filled"), 2).alias("avg_quantity_filled"),
    )
)

result = (
    totals
    .select(
        "quantity_filled_tier",
        "activity_month",
        "num_regions",
        "avg_quantity_filled",
    )
    .orderBy(F.col("activity_month").asc_nulls_last())
)
