from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city", "region").orderBy("is_new_york", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("is_new_york", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("city").orderBy(F.col("running_count_sqft").desc(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("sqft").between(8456, 11082))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.when(F.col("city") == "New York", 1).otherwise(0).alias("is_new_york"),
        F.date_format("opened_date", "yyyy-MM").alias("opened_date_label"),
    )
)

metrics = (
    filtered
    .filter(~F.col("region").isin(["South", "East", "Central"]))
    .select(
        "id",
        "store_name",
        "city",
        "opened_date_label",
        F.max("sqft").over(w_ordered).alias("running_max_sqft"),
        F.min("sqft").over(w_ordered).alias("running_min_sqft"),
        F.sum("sqft").over(w_ordered).alias("running_sum_sqft"),
        F.count("sqft").over(w_ordered).alias("running_count_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_3_prior"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

filtered_stores = (
    latest
    .filter(F.col("running_min_sqft") <= 15100)
    .select(
        "id",
        "opened_date_label",
        "running_max_sqft",
        "running_min_sqft",
        "running_sum_sqft",
        "running_count_sqft",
        "sqft_avg_3_prior",
        F.col("running_max_sqft").cast("int").alias("running_max_sqft_int"),
        (F.col("dedup_rank") - F.col("running_max_sqft")).alias("dedup_rank_minus_running_max_sqft"),
    )
)

with_latest = (
    filtered_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

valid = (
    with_latest
    .filter(F.month("inventory_last_restock").isin([1, 3, 6]))
    .select(
        "id",
        "opened_date_label",
        "running_max_sqft",
        "running_min_sqft",
        "running_sum_sqft",
        "running_count_sqft",
        "sqft_avg_3_prior",
        "running_max_sqft_int",
        "dedup_rank_minus_running_max_sqft",
        "inventory_last_restock",
        F.floor("running_sum_sqft").alias("running_sum_sqft_whole"),
        F.when(F.col("dedup_rank_minus_running_max_sqft") >= 62, "High").when(F.col("dedup_rank_minus_running_max_sqft") >= 30, "Medium").otherwise("Low").alias("dedup_rank_minus_running_max_sqft_tier"),
    )
)

report = (
    valid
    .select(
        "id",
        "opened_date_label",
        "running_max_sqft",
        "running_min_sqft",
        "sqft_avg_3_prior",
        "dedup_rank_minus_running_max_sqft",
        "inventory_last_restock",
        "running_sum_sqft_whole",
        "dedup_rank_minus_running_max_sqft_tier",
    )
    .orderBy(F.col("running_sum_sqft_whole").desc())
)

result = (
    report
    .select(
        "id",
        "sqft_avg_3_prior",
        "dedup_rank_minus_running_max_sqft",
        "running_sum_sqft_whole",
        "dedup_rank_minus_running_max_sqft_tier",
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("running_sum_sqft_whole").desc())
)
