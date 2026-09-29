from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "*",
        F.lead("revenue", 1).over(w).alias("next_revenue"),
        F.last("revenue", ignorenulls=True).over(w2).alias("last_known_revenue"),
        F.sum("revenue").over(w2).alias("cumulative_revenue"),
    )
)
