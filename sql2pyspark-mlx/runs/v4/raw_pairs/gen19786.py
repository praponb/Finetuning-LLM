from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "*",
        F.last("brand", ignorenulls=True).over(w).alias("last_known_brand"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
        F.last("price", ignorenulls=True).over(w2).alias("last_known_price"),
    )
)
