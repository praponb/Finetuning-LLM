from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_last_restock").desc(), "id")
w2 = Window.orderBy(F.col("last_restock").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy("quantity", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("store_id").orderBy(F.col("days_since_last_restock_bucket").desc_nulls_last())
w6 = Window.partitionBy("store_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("days_since_last_restock").desc())
w8 = Window.partitionBy("store_id").orderBy("seq_no", "id")
w_ordered = Window.partitionBy("store_id").orderBy("days_since_last_restock_bucket_percent_rank")

cleaned = (
    df_inventory
    .filter(F.col("product_id") == 10)
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

valid = (
    cleaned
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-08-12")))
    .select(
        "*",
        (F.col("quantity") - F.col("days_since_last_restock")).alias("quantity_minus_days_since_last_restock"),
    )
)

enriched = (
    valid
    .select(
        "id",
        "store_id",
        "quantity",
        "days_since_last_restock",
        "quantity_minus_days_since_last_restock",
        F.ntile(3).over(w).alias("days_since_last_restock_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(5).over(w3).alias("quantity_bucket"),
    )
)

enriched_inventory = (
    enriched
    .filter(F.col("store_id").isin([4, 6]))
    .select(
        "*",
        F.max("quantity_bucket").over(w4).alias("store_id_max_quantity_bucket"),
        F.percent_rank().over(w5).alias("days_since_last_restock_bucket_percent_rank"),
        F.sum("days_since_last_restock_bucket").over(w6).alias("running_days_since_last_restock_bucket"),
    )
)

cleaned_inventory = (
    enriched_inventory
    .filter(F.col("days_since_last_restock") >= 43)
    .withColumn("days_since_last_restock_times_days_since_last_restock_bucket_percent_rank", (F.col("days_since_last_restock") * F.col("days_since_last_restock_bucket_percent_rank")))
    .withColumn("days_since_last_restock_times_running_days_since_last_restock_bucket", (F.col("days_since_last_restock") * F.col("running_days_since_last_restock_bucket")))
)

with_windows = (
    cleaned_inventory
    .select(
        "id",
        "store_id",
        "days_since_last_restock",
        "seq_no",
        "quantity_bucket",
        "days_since_last_restock_bucket_percent_rank",
        "days_since_last_restock_times_days_since_last_restock_bucket_percent_rank",
        F.dense_rank().over(w7).alias("days_since_last_restock_dense_rank"),
        F.ntile(10).over(w8).alias("seq_no_bucket"),
        F.percent_rank().over(w_ordered).alias("days_since_last_restock_bucket_percent_rank_percent_rank"),
    )
)

joined = (
    with_windows
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.region"), F.col("d.opened_date"), F.col("d.city"))
)

result = (
    joined
    .groupBy("region")
    .agg(
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_rows"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
)
