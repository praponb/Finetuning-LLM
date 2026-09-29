from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.orderBy("inventory_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy(F.col("inventory_quantity").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

with_latest = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

with_windows = (
    with_latest
    .select(
        "*",
        F.last("inventory_quantity", ignorenulls=True).over(w2).alias("last_known_inventory_quantity"),
        F.last("inventory_quantity", ignorenulls=True).over(w3).alias("last_known_inventory_quantity_2"),
    )
)

deduped = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    deduped
    .filter(F.col("last_restock") >= "2024-11-27")
    .select(
        "*",
        F.when(F.col("last_known_inventory_quantity_2") >= 208, "High").when(F.col("last_known_inventory_quantity_2") >= 69, "Medium").otherwise("Low").alias("last_known_inventory_quantity_2_band"),
        F.dayofweek("last_restock").alias("last_restock_dayofweek"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date"))
)

result = (
    latest_match
    .groupBy("city")
    .agg(
        F.round(F.avg("last_known_inventory_quantity"), 2).alias("avg_last_known_inventory_quantity"),
        F.sum(F.when(F.col("city") == "Berlin", F.col("last_restock_dayofweek"))).alias("berlin_total_last_restock_dayofweek"),
    )
    .orderBy(F.col("avg_last_known_inventory_quantity").desc_nulls_last())
)
