from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("cost").desc_nulls_last(), "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.avg("shipping_fee").alias("avg_order_shipping_fee"))

cleaned = (
    df_inventory
    .filter(F.col("last_restock") >= F.lit("2023-03-22").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.when(F.col("quantity") >= 268, "High").when(F.col("quantity") >= 32, "Medium").otherwise("Low").alias("quantity_level"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.cost"), F.col("b.product_name"))
)

latest = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "store_id", "product_id", "quantity", "last_restock", "cost")
)

filtered = (
    latest
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-07-22"))) & F.col("store_id").isin([1, 6]))
    .select(
        "*",
        F.round("cost", 0).alias("cost_rounded"),
        F.floor("quantity").alias("quantity_whole"),
    )
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "cost",
        "cost_rounded",
        "latest_rank",
        F.col("avg_order_shipping_fee"),
    )
    .orderBy(F.col("cost_rounded").asc_nulls_first(), "avg_order_shipping_fee")
)
