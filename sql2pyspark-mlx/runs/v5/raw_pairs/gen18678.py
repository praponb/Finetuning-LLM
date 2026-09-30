from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc(), "id")
w3 = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy(F.col("last_known_quantity").desc())

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_num", F.col("quantity").cast("string"))
)

enriched = (
    cleaned
    .filter(F.col("product_id") == 40)
    .withColumn("quantity_avg_2_prior", F.avg("quantity").over(w))
    .withColumn("unit_price_bucket", F.ntile(4).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w4))
)

joined = (
    enriched
    .alias("e")
    .join(df_orders.alias("p"), F.col("e.order_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.discount"), F.col("p.amount"))
)

matched_keys = (
    joined
    .select("id")
    .intersect(
        df_order_items
        .filter(F.col("order_id").isin([6, 16, 26, 27]))
        .select("id")
    )
)

verified = (
    joined
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "order_id",
        "quantity",
        "unit_price",
        "quantity_avg_2_prior",
        "unit_price_bucket",
        "event_seq",
        "last_known_quantity",
        "discount",
        F.cume_dist().over(w5).alias("last_known_quantity_cume_dist"),
    )
)
