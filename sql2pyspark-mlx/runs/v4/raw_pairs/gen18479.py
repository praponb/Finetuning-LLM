from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")
w4 = Window.partitionBy("category", "brand")

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat_ws("-", "product_name", "brand").alias("product_name_brand_key"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)

result = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
    .withColumn("corr_stock_qty_price", F.corr("stock_qty", "price").over(w4))
)
