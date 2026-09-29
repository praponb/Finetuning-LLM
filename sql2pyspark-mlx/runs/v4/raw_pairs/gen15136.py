from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("adjusted_unit_price", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select("*", F.round((F.col("unit_price") * 0.9), 2).alias("adjusted_unit_price"))
)

result = (
    cleaned
    .select(
        "*",
        F.first("unit_price", ignorenulls=True).over(w_grp).alias("first_known_unit_price"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
