from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("store_sale_units").desc_nulls_first())
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("opened_date", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

eligible = (
    df_stores
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 6), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.opened_date", F.col("c.units").alias("store_sale_units"))
)

with_windows = (
    with_last
    .withColumn("store_sale_units_rank", F.rank().over(w2))
    .withColumn("cumulative_store_sale_units", F.sum("store_sale_units").over(w3))
    .withColumn("prev_store_sale_units", F.lag("store_sale_units", 1).over(w4))
)

kept = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.last_restock") <= F.col("s.opened_date")), "left_anti")
)

with_last_stores = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.store_sale_units",
        F.col("c.sale_date"),
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

remaining_keys = (
    with_last_stores
    .select("id")
    .subtract(
        df_stores
        .filter(F.col("region").isin(["West", "East"]))
        .select("id")
    )
)

verified = (
    with_last_stores
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("opened_date", "store_sale_units", "sale_date", "store_sale_revenue")
)
