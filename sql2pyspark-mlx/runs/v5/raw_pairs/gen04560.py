from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") - F.lag("units", 2).over(w)).alias("units_change"),
        F.sum("revenue").over(w2).alias("running_revenue"),
    )
)
