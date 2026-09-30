from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date_minus_36d").desc(), "id")
w2 = Window.partitionBy("category").orderBy("launch_date_minus_36d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy("launch_date_minus_36d", "id")
w4 = Window.partitionBy("brand").orderBy("launch_date_minus_36d", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("brand").orderBy(F.col("seq_no").desc(), "id")
w6 = Window.orderBy("event_seq_2", "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("stock_qty").between(101, 275))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        F.date_sub("launch_date", 36).alias("launch_date_minus_36d"),
        F.regexp_replace("product_name", " ", "_").alias("product_name_slug"),
    )
)

latest = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    latest
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("first_known_brand", F.first("brand", ignorenulls=True).over(w2))
    .withColumn("event_seq_2", F.row_number().over(w3))
    .withColumn("first_known_brand_2", F.first("brand", ignorenulls=True).over(w4))
)

scored_products = (
    scored
    .withColumn("seq_no_2", F.row_number().over(w5))
    .withColumn("running_seq_no", F.sum("seq_no").over(w2))
    .withColumn("prev_event_seq_2", F.lag("event_seq_2", 1, 0).over(w6))
)

eligible = (
    scored_products
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.launch_date_minus_36d")), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date_minus_36d")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue").alias("store_sale_revenue"))
)

latest_match_products = (
    latest_match
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date_minus_36d")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

result = (
    latest_match_products
    .select(
        "id",
        "category",
        "brand",
        "product_name_slug",
        "seq_no",
        "first_known_brand",
        "first_known_brand_2",
        "running_seq_no",
        "prev_event_seq_2",
    )
)
