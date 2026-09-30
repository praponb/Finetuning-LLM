from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_order_items
    .filter(F.col("quantity") < 7)
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price",
        F.round((F.col("quantity") / F.sum("quantity").over(w)), 3).alias("quantity_share"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)
