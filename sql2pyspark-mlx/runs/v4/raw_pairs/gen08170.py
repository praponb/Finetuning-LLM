from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy("launch_date", "id")

result = (
    df_products
    .select(
        "id",
        "launch_date",
        "price",
        "brand",
        F.sum("price").over(w).alias("running_price"),
        F.lead("price", 1).over(w2).alias("next_price"),
    )
)
