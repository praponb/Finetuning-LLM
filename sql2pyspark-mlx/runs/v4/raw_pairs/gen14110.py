from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("category")
w4 = Window.partitionBy("category", "supplier_id")

with_windows = (
    df_products
    .withColumn("running_stock_qty", F.sum("stock_qty").over(w))
    .withColumn("last_known_stock_qty", F.last("stock_qty", ignorenulls=True).over(w2))
    .withColumn("category_max_stock_qty", F.max("stock_qty").over(w3))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w4))
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(wireless mouse|Noise-Cancel Headphones)$"), "GROUP_B").otherwise("CORE").alias("product_name_zone"),
    )
    .orderBy(F.col("cost").desc(), "product_name_zone")
)
