from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")
w2 = Window.partitionBy("category")

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "price",
        "category",
        "cost",
        (F.col("price") - F.lag("price", 2).over(w)).alias("price_change"),
        F.avg("cost").over(w2).alias("category_avg_cost"),
    )
)
