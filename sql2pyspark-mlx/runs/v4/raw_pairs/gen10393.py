from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand")
w2 = Window.partitionBy("supplier_id").orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "*",
        F.round((F.col("cost") / F.sum("cost").over(w)), 4).alias("cost_share"),
        F.first("stock_qty", ignorenulls=True).over(w2).alias("first_known_stock_qty"),
    )
)
