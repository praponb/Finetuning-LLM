from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("city").orderBy(F.col("inventory_last_restock").desc_nulls_first(), "id")

scored = (
    df_stores
    .withColumn("sqft_avg_5", F.avg("sqft").over(w))
    .withColumn("running_sqft", F.sum("sqft").over(w2))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.quantity") > 78), "left_semi")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

ranked = (
    with_latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

kept = (
    ranked
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id") == 20), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        "sqft_avg_5",
        "running_sqft",
        "inventory_quantity",
        "inventory_last_restock",
        F.when(F.col("store_name").rlike("^(Airport Kiosk|Siam Flagship)$"), "GROUP_A").when(F.col("store_name").rlike("^(Downtown Express|Mall Central)$"), "CORE").otherwise("EXTENDED").alias("store_name_zone"),
    )
    .orderBy(F.col("sqft_avg_5").desc(), "region")
)
