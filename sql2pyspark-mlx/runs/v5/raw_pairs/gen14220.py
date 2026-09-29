from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("cost") >= 290)
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        "supplier_id",
        "price",
        F.lead("cost", 1).over(w).alias("next_cost"),
        (F.col("price") - F.lag("price", 1, 0).over(w2)).alias("price_change"),
    )
    .orderBy(F.col("supplier_id").desc(), F.col("cost").desc())
)
