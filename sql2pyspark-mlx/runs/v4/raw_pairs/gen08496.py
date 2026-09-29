from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("price").between(600, 1000))
    .select(
        "id",
        "category",
        "launch_date",
        "price",
        F.lead("price", 2).over(w).alias("next_price"),
    )
    .orderBy(F.col("price").desc())
)
