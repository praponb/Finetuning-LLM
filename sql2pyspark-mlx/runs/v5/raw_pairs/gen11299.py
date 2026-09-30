from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("store_id").orderBy("last_restock", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("store_region").orderBy("quantity", "id")

with_windows = (
    df_inventory
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.first("quantity", ignorenulls=True).over(w).alias("first_known_quantity"),
        F.first("quantity", ignorenulls=True).over(w_part).alias("first_known_quantity_2"),
        F.avg("quantity").over(w_grp).alias("quantity_avg_3_prior"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.quantity",
        "s.first_known_quantity",
        "s.first_known_quantity_2",
        F.col("b.opened_date"),
        F.col("b.region"),
    )
)

enriched_inventory = (
    enriched
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.region").alias("store_region"))
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.sqft") == 4924)
    .select("a.*", F.col("s.store_name"))
)

enriched_inventory_3 = (
    enriched_inventory_2
    .withColumn("next_quantity", F.lead("quantity").over(w4))
    .withColumn("prev_first_known_quantity_2", F.lag("first_known_quantity_2").over(w4))
    .withColumn("row_seq", F.row_number().over(w4))
)

result = (
    enriched_inventory_3
    .select(
        "id",
        "store_id",
        "quantity",
        "first_known_quantity",
        "first_known_quantity_2",
        "opened_date",
        "store_name",
        "prev_first_known_quantity_2",
        "row_seq",
    )
    .orderBy("first_known_quantity_2")
)
