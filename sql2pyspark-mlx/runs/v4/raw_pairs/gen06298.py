from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("cost", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")
w_ordered = Window.partitionBy("supplier_id")
w_grp = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last())

enriched = (
    df_products
    .withColumn("cost_bucket", F.ntile(5).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("corr_cost_price", F.corr("cost", "price").over(w_ordered))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w_ordered))
    .withColumn("stock_qty_rank", F.rank().over(w_grp))
)

result = (
    enriched
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Backpack XL|Coffee Maker)$"), "GROUP_B").otherwise("GROUP_A").alias("product_name_zone"),
    )
    .orderBy("supplier_id")
)
