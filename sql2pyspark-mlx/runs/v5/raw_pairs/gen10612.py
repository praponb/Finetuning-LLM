from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "cost",
        "stock_qty",
        F.sum("cost").over(w).alias("cumulative_cost"),
        F.last("cost", ignorenulls=True).over(w2).alias("last_known_cost"),
    )
)
