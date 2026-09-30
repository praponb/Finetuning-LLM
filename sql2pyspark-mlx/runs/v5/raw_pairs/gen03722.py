from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("city").orderBy("opened_date", "id")
w2 = Window.partitionBy("region").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("next_sqft", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("region")
w5 = Window.partitionBy("region").orderBy(F.col("running_next_sqft").desc(), "id")
w6 = Window.partitionBy("region").orderBy("row_seq", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("region").orderBy("next_sqft_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("region").orderBy("running_next_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_part = Window.orderBy("next_sqft_share", "id").rowsBetween(-6, Window.currentRow)

scored = (
    df_stores
    .select(
        "id",
        "store_name",
        "region",
        F.lead("sqft").over(w_ordered).alias("next_sqft"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([4, 5, 6, 9])), "left_anti")
)

filtered = (
    kept
    .filter(~F.col("region").isin(["Central", "North", "West"]))
    .select(
        "id",
        "store_name",
        "region",
        "next_sqft",
        "row_seq",
        F.upper(F.trim("store_name")).alias("store_name_normalized"),
        F.coalesce("region", F.lit("OTHER")).alias("region_clean"),
    )
)

metrics = (
    filtered
    .filter(F.col("row_seq") >= 7)
    .withColumn("running_next_sqft", F.sum("next_sqft").over(w2))
    .withColumn("next_sqft_avg_6", F.avg("next_sqft").over(w3))
    .withColumn("last_known_row_seq", F.last("row_seq", ignorenulls=True).over(w2))
)

scored_stores = (
    metrics
    .withColumn("next_sqft_share", F.round((F.col("next_sqft") / F.sum("next_sqft").over(w4)), 4))
    .withColumn("running_next_sqft_bucket", F.ntile(5).over(w5))
    .withColumn("next_sqft_avg_6_avg_5", F.avg("next_sqft_avg_6").over(w6))
    .withColumn("running_row_seq", F.sum("row_seq").over(w7))
    .withColumn("cumulative_last_known_row_seq", F.sum("last_known_row_seq").over(w8))
)

latest_match = (
    scored_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w9))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.next_sqft",
        "s.store_name_normalized",
        "s.running_next_sqft",
        "s.next_sqft_avg_6",
        "s.last_known_row_seq",
        "s.next_sqft_share",
        "s.running_next_sqft_bucket",
        "s.next_sqft_avg_6_avg_5",
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

filtered_stores = (
    latest_match
    .filter(F.col("next_sqft_share") < 0.32)
    .select(
        "id",
        "next_sqft",
        "store_name_normalized",
        "running_next_sqft",
        "last_known_row_seq",
        "next_sqft_share",
        "running_next_sqft_bucket",
        "next_sqft_avg_6_avg_5",
        "inventory_quantity",
        (F.col("next_sqft_avg_6_avg_5") - F.col("next_sqft_share")).alias("next_sqft_avg_6_avg_5_minus_next_sqft_share"),
        F.coalesce("region", F.lit("OTHER")).alias("resolved_region"),
    )
)

result = (
    filtered_stores
    .select(
        "id",
        "next_sqft",
        "store_name_normalized",
        "last_known_row_seq",
        "running_next_sqft_bucket",
        "next_sqft_avg_6_avg_5",
        "inventory_quantity",
        "resolved_region",
        F.sum("inventory_quantity").over(w_part).alias("last7_sum_inventory_quantity"),
        F.count("inventory_quantity").over(w_part).alias("last7_count_inventory_quantity"),
        F.min("inventory_quantity").over(w_part).alias("last7_min_inventory_quantity"),
    )
)
