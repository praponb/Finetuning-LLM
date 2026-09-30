from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("row_num").desc(), "id")
w3 = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id", "store_id").orderBy("running_quantity", "id")

valid = (
    df_inventory
    .filter(F.col("quantity").between(89, 232))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.when(F.col("quantity") >= 268, "High").when(F.col("quantity") >= 42, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

latest = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("running_quantity", F.sum("quantity").over(w3))
)

enriched = (
    scored
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.store_name"))
)

ranked = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched_inventory = (
    ranked
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.cost"), F.col("b.brand"), F.col("b.price"))
)

result = (
    enriched_inventory
    .select(
        "id",
        "quantity",
        "quantity_level",
        "running_quantity",
        "store_name",
        "latest_rank",
        "cost",
        "brand",
        "price",
    )
    .orderBy("quantity", "latest_rank")
)
