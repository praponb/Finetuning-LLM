from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("category").orderBy(F.col("cost").desc())

result = (
    df_products
    .filter(F.year("launch_date") >= 2025)
    .select(
        "id",
        "supplier_id",
        "brand",
        "launch_date",
        "price",
        "category",
        "cost",
        F.lead("price", 1).over(w).alias("next_price"),
        F.rank().over(w2).alias("cost_rank"),
    )
)
