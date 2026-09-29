from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id")
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")

with_windows = (
    df_products
    .select(
        "*",
        F.lag("price", 1).over(w).alias("prev_price"),
        F.lead("cost", 1).over(w2).alias("next_cost"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "next_cost",
        F.when(F.col("product_name").rlike("^(Yoga Mat|Noise-Cancel Headphones)$"), "GROUP_B").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "CORE").otherwise("EXTENDED").alias("product_name_zone"),
    )
)
