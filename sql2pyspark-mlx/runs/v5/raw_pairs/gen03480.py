from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("quantity")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w5 = Window.partitionBy("store_id").orderBy("quantity_rank", "id")
w6 = Window.partitionBy("store_id").orderBy("quantity_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.orderBy(F.col("quantity_avg_2").desc_nulls_first())
w9 = Window.orderBy("seq_no", "id")

cleaned = (
    df_inventory
    .filter(F.col("last_restock") < "2025-03-01")
    .select(
        "*",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label_2"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "store_id",
        "quantity",
        "last_restock_label_2",
        F.first("quantity", ignorenulls=True).over(w).alias("first_known_quantity"),
        F.avg("quantity").over(w2).alias("quantity_avg_2"),
        F.rank().over(w3).alias("quantity_rank"),
        F.cume_dist().over(w4).alias("quantity_cume_dist"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.store_name"), F.col("d.opened_date"), F.col("d.sqft"))
)

scored_inventory = (
    with_dims
    .select(
        "id",
        "quantity_avg_2",
        "opened_date",
        "sqft",
        F.row_number().over(w5).alias("seq_no"),
        F.sum("sqft").over(w6).alias("cumulative_sqft"),
    )
)

scored_inventory_2 = (
    scored_inventory
    .select(
        "id",
        "sqft",
        "seq_no",
        F.sum("sqft").over(w7).alias("running_sqft"),
        F.rank().over(w_part).alias("quantity_avg_2_rank"),
        F.ntile(3).over(w9).alias("seq_no_bucket"),
    )
)

result = (
    scored_inventory_2
    .select(
        "id",
        "sqft",
        "seq_no",
        "running_sqft",
        "quantity_avg_2_rank",
        "seq_no_bucket",
    )
)
