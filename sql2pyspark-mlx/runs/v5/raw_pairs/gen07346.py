from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("unit_price").desc(), "id")
w2 = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("order_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand")
w6 = Window.partitionBy("brand").orderBy("cost", "id")
w_hist = Window.partitionBy("order_id").orderBy("quantity_avg_6_prior", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.partitionBy("brand").orderBy("unit_price_share", "id")
w9 = Window.partitionBy("order_id").orderBy(F.col("cost_avg_6").desc_nulls_first(), "id")
w10 = Window.partitionBy("brand", "order_id").orderBy(F.col("unit_price_bucket").desc_nulls_last())
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.avg("unit_price").alias("avg_order_item_unit_price"))

valid = (
    df_order_items
    .filter(F.col("product_id").isin([24, 25, 27]))
    .select("*", F.col("unit_price").cast("int").alias("unit_price_int"))
)

enriched = (
    valid
    .withColumn("unit_price_bucket", F.ntile(10).over(w))
    .withColumn("quantity_avg_6_prior", F.avg("quantity").over(w2))
    .withColumn("first_known_unit_price_int", F.first("unit_price_int", ignorenulls=True).over(w3))
    .withColumn("first_known_unit_price", F.first("unit_price", ignorenulls=True).over(w_running))
)

with_dims = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.order_id",
        "src.product_id",
        "src.unit_price",
        "src.unit_price_bucket",
        "src.quantity_avg_6_prior",
        F.col("b.cost"),
        F.col("b.brand"),
    )
)

with_windows = (
    with_dims
    .withColumn("unit_price_share", F.round((F.col("unit_price") / F.sum("unit_price").over(w5)), 4))
    .withColumn("prev_unit_price_bucket", F.lag("unit_price_bucket", 1).over(w6))
)

metrics = (
    with_windows
    .filter(F.col("unit_price_share") < 0.64)
    .select(
        "id",
        "order_id",
        "unit_price_bucket",
        "brand",
        "unit_price_share",
        F.avg("cost").over(w_hist).alias("cost_avg_6"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("row_num_2", F.rank().over(w10))
    .filter(F.col("row_num_2") == 1)
    .select("id", "order_id", "brand", "cost_avg_6", "event_seq", "row_num")
)

result = (
    deduped
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "cost_avg_6",
        "event_seq",
        F.col("avg_order_item_unit_price"),
    )
    .orderBy(F.col("cost_avg_6").asc_nulls_last())
)
