from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .select(
        "*",
        F.last("units", ignorenulls=True).over(w).alias("last_known_units"),
        F.row_number().over(w_running).alias("row_seq"),
    )
)
