from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc(), "id")

result = (
    df_products
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        "price",
        F.lag("cost", 1, 0).over(w).alias("prev_cost"),
        F.row_number().over(w2).alias("rn"),
    )
    .orderBy("prev_cost")
)
