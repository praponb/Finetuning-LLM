from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .filter(F.col("revenue") < 1400)
    .select(
        "id",
        "product_id",
        "store_id",
        "sale_date",
        "revenue",
        F.row_number().over(w).alias("rn"),
        F.sum("revenue").over(w2).alias("running_revenue"),
    )
)
