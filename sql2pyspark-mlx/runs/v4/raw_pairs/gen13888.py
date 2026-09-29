from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("cost") <= 600)
    .select(
        "id",
        "category",
        "launch_date",
        "price",
        F.lag("price", 2).over(w).alias("prev_price"),
    )
)
