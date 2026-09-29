from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("last_restock", "id")
w3 = Window.orderBy(F.col("opened_date").desc(), "id")
w4 = Window.orderBy("last_restock_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id", "region").orderBy("last_restock_plus_months", "id")
w6 = Window.partitionBy("region")
w7 = Window.partitionBy("store_id").orderBy("seq_no", "id")
w8 = Window.partitionBy("store_id").orderBy(F.col("row_seq").desc(), "id")
w9 = Window.partitionBy("region").orderBy("last_restock_plus_months", "id")
w10 = Window.partitionBy("store_id").orderBy("row_seq_bucket", "id")
w11 = Window.partitionBy("store_id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-08-16").cast("date"))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.add_months("last_restock", 12).alias("last_restock_plus_months"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.store_name"), F.col("d.opened_date"), F.col("d.region"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.last("region", ignorenulls=True).over(w).alias("last_known_region"),
        F.row_number().over(w_running).alias("seq_no"),
        F.row_number().over(w3).alias("row_seq"),
        F.last("region", ignorenulls=True).over(w4).alias("last_known_region_2"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    latest
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w6)), 4))
    .withColumn("region_max_seq_no", F.max("seq_no").over(w6))
    .withColumn("seq_no_bucket", F.ntile(4).over(w7))
    .withColumn("row_seq_bucket", F.ntile(3).over(w8))
)

scored = (
    metrics
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("event_seq_2", F.row_number().over(w10))
    .withColumn("store_id_count_seq_no", F.count("seq_no").over(w11))
)

by_type = (
    scored
    .groupBy("store_id")
    .pivot("region", ["Central", "East"])
    .agg(F.sum("store_id_count_seq_no").alias("total"))
    .select("store_id", F.col("Central").alias("central"), F.col("East").alias("east"))
)

result = (
    by_type
    .select("store_id", "east")
    .orderBy(F.col("store_id").desc_nulls_last())
)
