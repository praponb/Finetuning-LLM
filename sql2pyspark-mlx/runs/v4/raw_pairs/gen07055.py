from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy("last_restock_month_end", "id")
w_running = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("city").orderBy("last_restock_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("store_id").isin([1, 4, 5]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

ranked = (
    base
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

cleaned = (
    ranked
    .filter(F.col("quantity") >= 68)
    .select(
        "id",
        "store_id",
        "last_restock_month_end",
        F.when(F.col("quantity") >= 200, "High").when(F.col("quantity") >= 101, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.opened_date"), F.col("p2.city"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.last("city", ignorenulls=True).over(w_running).alias("last_known_city"),
        F.last("city", ignorenulls=True).over(w_ordered).alias("last_known_city_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "last_restock_month_end",
        "quantity_band",
        "opened_date",
        "seq_no",
        "last_known_city",
        "last_known_city_2",
    )
    .orderBy("id")
    .limit(10)
)
