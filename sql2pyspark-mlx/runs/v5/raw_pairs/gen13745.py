from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id")
revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

enriched = (
    df_inventory
    .filter(F.col("last_restock") < "2025-05-01")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.lag("quantity").over(w).alias("prev_quantity"),
        F.lead("quantity").over(w).alias("next_quantity"),
        F.row_number().over(w).alias("event_seq"),
    )
)

result = (
    enriched
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "prev_quantity",
        "next_quantity",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy(F.col("sum_store_sale_revenue").desc())
)
