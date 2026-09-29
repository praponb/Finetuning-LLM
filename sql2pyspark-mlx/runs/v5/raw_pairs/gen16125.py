from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

with_last = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.region",
        "s.opened_date",
        "s.sqft",
        F.col("c.status").alias("order_status"),
        F.col("c.discount").alias("order_discount"),
    )
)

with_last_stores = (
    with_last
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

with_latest = (
    with_last_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

latest_match = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date"), F.col("c.revenue"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([4, 5])), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "region",
        "opened_date",
        "sqft",
        "order_discount",
        "inventory_last_restock",
        F.when(F.col("region").rlike("^(West|North)$"), "GROUP_B").when(F.col("region").rlike("^(South|East)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("region_zone"),
    )
)
