from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("last_restock_plus_months", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock_plus_months", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("store_id", "product_id").orderBy("last_known_quantity", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("last_known_quantity", "id")
w6 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id", "product_id").orderBy("last_known_quantity_2", "id")

valid = (
    df_inventory
    .filter(F.col("product_id").isin([4, 5, 7, 8]))
    .select(
        "*",
        F.when(F.col("quantity") >= 195, "High").when(F.col("quantity") >= 80, "Medium").otherwise("Low").alias("quantity_band"),
    )
)

cleaned = (
    valid
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-12-05")))
    .select(
        "*",
        F.add_months("last_restock", 1).alias("last_restock_plus_months"),
        F.when(F.col("quantity") >= 193, "High").when(F.col("quantity") >= 76, "Medium").otherwise("Low").alias("quantity_band_2"),
    )
)

enriched = (
    cleaned
    .select(
        "*",
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity"),
        F.lag("quantity", 1).over(w2).alias("prev_quantity"),
        F.last("quantity", ignorenulls=True).over(w).alias("last_known_quantity_2"),
        F.avg("quantity").over(w3).alias("quantity_avg_6"),
    )
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.avg("quantity").over(w4).alias("quantity_avg_3"),
        F.lead("last_known_quantity", 1).over(w5).alias("next_last_known_quantity"),
        F.first("last_known_quantity_2", ignorenulls=True).over(w6).alias("first_known_last_known_quantity_2"),
        F.row_number().over(w7).alias("seq_no"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "store_id",
        "last_restock",
        "last_restock_plus_months",
        "quantity_band_2",
        "prev_quantity",
        "last_known_quantity_2",
        "quantity_avg_6",
        "seq_no",
    )
    .orderBy(
        F.col("last_known_quantity_2").desc_nulls_last(),
        F.col("prev_quantity").desc(),
    )
)
