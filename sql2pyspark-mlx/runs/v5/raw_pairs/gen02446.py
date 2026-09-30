from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w_grp = Window.partitionBy("store_id").orderBy("quantity_filled", "id")
w_recent = Window.partitionBy("store_id").orderBy("quantity_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-24").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "store_id", "quantity")
)

base = (
    deduped
    .filter(F.col("quantity").isNotNull())
    .withColumn("quantity_whole", F.ceil("quantity"))
    .withColumn("quantity_filled", F.coalesce("quantity", F.lit(0)))
)

scored = (
    base
    .select(
        "id",
        "quantity_whole",
        F.lead("quantity_whole", 1).over(w_grp).alias("next_quantity_whole"),
        F.sum("quantity").over(w_recent).alias("cumulative_quantity"),
        F.max("quantity").over(w4).alias("store_id_max_quantity"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("next_quantity_whole") >= 90, "PREMIUM").otherwise("STANDARD").alias("next_quantity_whole_tier"),
    )
    .agg(
        F.percentile_approx("next_quantity_whole", [0.25, 0.5, 0.75]).alias("next_quantity_whole_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("quantity_whole"), 2).alias("avg_quantity_whole"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
        F.avg("next_quantity_whole").alias("avg_next_quantity_whole"),
    )
    .filter(F.col("distinct_ids_3") >= 1)
)
