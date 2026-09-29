from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("last_restock_period", "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy("island_id", "id")
w4 = Window.partitionBy("store_id").orderBy("rn")
w5 = Window.partitionBy("product_id").orderBy("island_id", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-04-15").cast("date"))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 12))
)

prev_events = valid.withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessions_inventory = with_gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

valid_inventory = (
    sessions_inventory
    .filter(F.col("island_id") <= 3)
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "last_restock_plus_months",
        "prev_last_restock_period",
        "new_group_flag",
        "island_id",
        F.round((F.col("new_group_flag") * 0.85), 2).alias("adjusted_new_group_flag"),
    )
)

ranked = (
    valid_inventory
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("row_num", F.rank().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    deduped
    .filter(F.col("island_id") > 7)
    .select(
        "id",
        "last_restock",
        "last_restock_plus_months",
        "prev_last_restock_period",
        "new_group_flag",
        "adjusted_new_group_flag",
        F.avg("adjusted_new_group_flag").over(w5).alias("last4_avg_adjusted_new_group_flag"),
        F.count("adjusted_new_group_flag").over(w5).alias("last4_count_adjusted_new_group_flag"),
        F.min("adjusted_new_group_flag").over(w5).alias("last4_min_adjusted_new_group_flag"),
    )
)
