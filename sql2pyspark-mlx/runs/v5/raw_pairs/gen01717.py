from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category").orderBy(F.col("cost").desc_nulls_first(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w_ordered = Window.orderBy("launch_date", "id")
w6 = Window.orderBy("price", "id")
w7 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("price_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy(F.col("row_seq").desc(), "id")
w10 = Window.orderBy(F.col("price").asc_nulls_first())

cleaned = (
    df_products
    .filter(F.col("product_name").like("Wa%"))
    .select("*", F.round((F.col("price") / F.col("cost")), 2).alias("margin"))
)

enriched = (
    cleaned
    .select(
        "id",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "margin",
        F.row_number().over(w_hist).alias("event_seq"),
        F.sum("price").over(w2).alias("cumulative_price"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") >= "2025-07-01"), "left_anti")
)

with_last = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.price",
        "s.launch_date",
        "s.cumulative_price",
        "s.row_num",
        F.col("c.revenue").alias("store_sale_revenue"),
    )
)

metrics = (
    with_last
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("price_bucket", F.ntile(3).over(w6))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

result = (
    metrics
    .select(
        "id",
        "price",
        "launch_date",
        F.sum("price").over(w7).alias("cumulative_price_2"),
        F.sum("seq_no").over(w8).alias("running_seq_no"),
        F.row_number().over(w9).alias("seq_no_2"),
        F.percent_rank().over(w10).alias("price_percent_rank"),
    )
)
