from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.brand",
        "s.launch_date",
        "s.tags",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_latest
    .groupBy(
        F.when(F.col("unit_price") >= 380, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("unit_price")).otherwise(0)).alias("electronics_unit_price"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("order_item_quantity")).otherwise(0)).alias("fashion_order_item_quantity"),
        F.avg(F.when(F.col("brand") == "Initech", F.col("order_item_quantity"))).alias("initech_avg_order_item_quantity"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("fashion_order_item_quantity").desc())
)
