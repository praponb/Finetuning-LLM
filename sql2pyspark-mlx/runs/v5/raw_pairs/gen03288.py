from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("product_id", "store_id").orderBy("last_restock", "id")
w_running = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("next_last_restock_year").desc_nulls_last(), "id")

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-07-27").cast("date"))
    .withColumn("last_restock_year", F.year("last_restock"))
)

metrics = (
    valid
    .filter(F.col("store_id").isin([1, 3, 5]))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.lag("last_restock_year").over(w_hist).alias("prev_last_restock_year"),
        F.lead("last_restock_year").over(w_hist).alias("next_last_restock_year"),
        F.lead("quantity").over(w_hist).alias("next_quantity"),
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("last_restock_year").over(w_running).alias("running_last_restock_year"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

filtered = (
    ranked
    .filter(F.col("last_restock") > F.lit("2023-04-14").cast("date"))
    .select(
        "id",
        "last_restock",
        "prev_last_restock_year",
        "next_last_restock_year",
        "running_last_restock_year",
        (F.col("running_last_restock_year") + F.col("next_last_restock_year")).alias("running_last_restock_year_plus_next_last_restock_year"),
    )
)

result = (
    filtered
    .withColumn("prev_last_restock_year_tier", F.when(F.col("prev_last_restock_year") >= 88, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_last_restock_year_tier", F.when(F.col("running_last_restock_year") >= 445, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["prev_last_restock_year_tier", "running_last_restock_year_tier"], ["prev_last_restock_year_tier"]],
        "prev_last_restock_year_tier",
        "running_last_restock_year_tier",
    )
    .agg(
        F.sum("running_last_restock_year_plus_next_last_restock_year").alias("total_running_last_restock_year_plus_next_last_restock_year"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("running_last_restock_year_plus_next_last_restock_year").alias("min_running_last_restock_year_plus_next_last_restock_year"),
        F.avg("running_last_restock_year_plus_next_last_restock_year").alias("avg_running_last_restock_year_plus_next_last_restock_year"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
