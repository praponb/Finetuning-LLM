from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_last(), "id")
w2 = Window.orderBy("quantity", "id")

base = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "quantity")
)

metrics = (
    ranked
    .select(
        "id",
        "quantity",
        F.lead("quantity").over(w2).alias("next_quantity"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

kept_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_order_items
        .filter(F.col("product_id").isin([36, 37]))
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy(
        F.when(F.col("quantity") >= 7, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
        F.when(F.col("next_quantity") >= 8, "HIGH").otherwise("LOW").alias("next_quantity_tier"),
        F.when(F.col("row_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("row_seq_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity"),
        F.percentile_approx("row_seq", 0.5).alias("row_seq_p50"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.round(F.avg("next_quantity"), 2).alias("avg_next_quantity_2"),
    )
    .orderBy(F.col("quantity_p90").asc_nulls_last())
)
