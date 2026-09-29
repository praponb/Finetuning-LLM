from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_stores
    .filter(F.col("sqft") >= 12593)
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.date_trunc("MONTH", "opened_date").alias("opened_date_period"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.opened_date_period")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

with_latest_stores = (
    with_latest
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity_2"),
        F.col("c.last_restock"),
    )
)

result = (
    with_latest_stores
    .withColumn("inventory_quantity_2_tier", F.when(F.col("inventory_quantity_2") >= 268, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["inventory_quantity_2_tier", "city", "region"], ["inventory_quantity_2_tier", "city"], ["inventory_quantity_2_tier"]],
        "inventory_quantity_2_tier",
        "city",
        "region",
    )
    .agg(
        F.sum(F.when(F.col("city") == "Chiang Mai", F.col("inventory_quantity")).otherwise(0)).alias("chiang_mai_inventory_quantity"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.percentile_approx("inventory_quantity", [0.25, 0.5, 0.75]).alias("inventory_quantity_quartiles"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("inventory_quantity_2_tier").desc_nulls_first())
)
