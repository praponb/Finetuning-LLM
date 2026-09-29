from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").asc_nulls_first(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

deduped = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "store_id", "product_id", "units", "revenue")
)

result = (
    deduped
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.col("min_order_item_quantity"),
    )
    .orderBy(F.col("min_order_item_quantity").asc_nulls_first())
)
