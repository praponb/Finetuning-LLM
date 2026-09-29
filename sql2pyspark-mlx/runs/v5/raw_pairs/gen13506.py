from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("seq_no").asc_nulls_last())
w3 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("category").orderBy("price", "id")
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("price").alias("sum_product_price"))

base = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Globex"))))
    .withColumn("product_name_length", F.length("product_name"))
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("UNKNOWN")))
)

kept = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price") >= 150), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 4, 6])), "left_anti")
)

eligible_products = (
    eligible
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") <= 350), "left_anti")
)

with_windows = (
    eligible_products
    .filter(F.col("price") > 610)
    .withColumn("prev_stock_qty", F.lag("stock_qty").over(w))
    .withColumn("prev_product_name_length", F.lag("product_name_length").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

enriched = (
    with_windows
    .withColumn("seq_no_percent_rank", F.percent_rank().over(w2))
    .withColumn("running_price", F.sum("price").over(w3))
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        "supplier_id",
        "product_name_length",
        F.col("sum_product_price"),
    )
    .orderBy(F.col("supplier_id").desc_nulls_first())
)
