from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("last_restock_day", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy(F.col("last_restock_day").desc(), "id")
w6 = Window.partitionBy("product_id", "store_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id")

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-07-06").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.dayofmonth("last_restock").alias("last_restock_day"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.product_name"))
)

with_windows = (
    enriched
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w3))
    .withColumn("quantity_bucket", F.ntile(3).over(w4))
    .withColumn("last_restock_day_bucket", F.ntile(4).over(w5))
    .withColumn("last_restock_day_avg_2", F.avg("last_restock_day").over(w6))
)

enriched_inventory = (
    with_windows
    .select(
        "*",
        F.last("last_restock_day", ignorenulls=True).over(w7).alias("last_known_last_restock_day"),
        F.avg("event_seq").over(w8).alias("product_id_avg_event_seq"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "quantity",
        "last_restock",
        "last_restock_day",
        "row_num",
        "quantity_avg_6_prior",
        "last_restock_day_bucket",
        "last_restock_day_avg_2",
        "last_known_last_restock_day",
    )
    .orderBy(F.col("last_restock_day").desc_nulls_first())
)
