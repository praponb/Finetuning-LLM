from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city").orderBy("sqft", "id")
w4 = Window.orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("inventory_quantity").desc_nulls_last())

with_last = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

latest_match = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.opened_date",
        "s.sqft",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.customer_id").isin([18, 21])), "left_anti")
)

enriched = (
    kept
    .select(
        "id",
        "opened_date",
        "sqft",
        F.ntile(5).over(w3).alias("sqft_bucket"),
        F.avg("inventory_quantity").over(w4).alias("inventory_quantity_avg_3"),
        F.ntile(5).over(w3).alias("sqft_bucket_2"),
        F.sum("sqft").over(w5).alias("cumulative_sqft"),
        F.percent_rank().over(w6).alias("inventory_quantity_percent_rank"),
    )
)

valid = (
    enriched
    .filter(F.col("opened_date") <= "2025-01-01")
    .select(
        "*",
        F.round("inventory_quantity_avg_3", 0).alias("inventory_quantity_avg_3_rounded"),
    )
)

result = (
    valid
    .select(
        "id",
        "opened_date",
        "sqft",
        "sqft_bucket",
        "inventory_quantity_avg_3",
        "sqft_bucket_2",
    )
    .orderBy("inventory_quantity_avg_3", "opened_date")
)
