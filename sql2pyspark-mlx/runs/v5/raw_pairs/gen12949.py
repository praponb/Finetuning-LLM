from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock_month_end").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("seq_no").asc_nulls_last())
w5 = Window.partitionBy("store_id").orderBy(F.col("running_sum_quantity").desc(), "id")
w6 = Window.partitionBy("region").orderBy(F.col("event_count").desc())

valid = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-07-23").cast("date")) & F.month("last_restock").isin([2, 6, 8]))
    .withColumn("last_restock_label", F.date_format("last_restock", "MMM yyyy"))
    .withColumn("last_restock_month_end", F.last_day("last_restock"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .select(
        "id",
        "store_id",
        "last_restock_label",
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.sum("quantity").over(w2).alias("running_sum_quantity"),
        F.avg("quantity").over(w2).alias("running_avg_quantity"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.rank().over(w4))
    .filter(F.col("rn") == 1)
)

deduped_inventory = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

with_dims = (
    deduped_inventory
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.region"), F.col("d.opened_date"))
)

grouped = (
    with_dims
    .groupBy("region", F.date_trunc("WEEK", "opened_date").alias("week_start"))
    .agg(
        F.count_if(F.col("region") == "Central").alias("central_events"),
        F.percentile_approx("running_min_quantity", [0.25, 0.5, 0.75]).alias("running_min_quantity_quartiles"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("central_events") >= 1)
    .withColumn("event_count_percent_rank", F.percent_rank().over(w6))
)

result = (
    grouped
    .select(
        "region",
        "week_start",
        "central_events",
        "running_min_quantity_quartiles",
        "event_count_percent_rank",
        F.when(F.col("region").rlike("^(East|Central)$"), "GROUP_B").when(F.col("region").rlike("^(North|South)$"), "EXTENDED").otherwise("GROUP_A").alias("region_zone"),
    )
)
