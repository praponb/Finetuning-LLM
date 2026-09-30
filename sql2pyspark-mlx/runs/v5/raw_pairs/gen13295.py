from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("quantity").desc())
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("revenue").alias("min_store_sale_revenue"))

deduped = (
    df_order_items
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(revenue_by_product_id, on="product_id", how="left")
    .select("id", "order_id", "product_id", "quantity", F.col("min_store_sale_revenue"))
    .orderBy(F.col("product_id").desc(), "quantity")
)
