from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").asc_nulls_first())
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

ranked = (
    df_store_sales
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
