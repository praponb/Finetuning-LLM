from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .filter(F.col("stock_qty").isNotNull())
    .select(
        "id",
        "brand",
        "launch_date",
        "cost",
        F.lag("cost", 2).over(w).alias("prev_cost"),
    )
    .orderBy(F.col("id").desc())
)
