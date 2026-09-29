from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

joined = (
    deduped
    .alias("s")
    .join(df_suppliers.alias("p2"), F.col("s.supplier_id") == F.col("p2.id"), "left")
    .select("s.*", F.col("p2.rating"), F.col("p2.country"))
)

with_last = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.unit_price"),
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

result = (
    with_last
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "latest_rank",
        "country",
    )
    .orderBy(F.col("price").asc_nulls_last(), F.col("country").desc())
)
