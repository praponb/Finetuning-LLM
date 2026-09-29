from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units", "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("cost").desc_nulls_first(), "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("sale_date").desc(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

with_windows = (
    df_store_sales
    .withColumn("running_count_revenue", F.count("revenue").over(w))
    .withColumn("running_sum_revenue", F.sum("revenue").over(w))
    .withColumn("running_max_revenue", F.max("revenue").over(w))
    .withColumn("next_revenue", F.lead("revenue", 1).over(w2))
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.units",
        "a.revenue",
        "a.next_revenue",
        F.col("d.price"),
    )
)

joined = (
    with_dims
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty") != 232)
    .select("s.*", F.col("p.cost"))
)

deduped = (
    joined
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

joined_store_sales = (
    deduped
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.sqft"), F.col("p.store_name"))
)

deduped_store_sales = (
    joined_store_sales
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped_store_sales
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "next_revenue",
        "sqft",
        "rn",
        "row_num",
        F.col("sum_order_item_quantity"),
    )
)
