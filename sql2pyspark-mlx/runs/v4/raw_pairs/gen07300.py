from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.month("launch_date").isin([3, 11, 12]))
    .select(
        "id",
        "launch_date",
        "price",
        F.lead("price", 1, 0).over(w).alias("next_price"),
    )
    .orderBy(F.col("price").desc())
)
