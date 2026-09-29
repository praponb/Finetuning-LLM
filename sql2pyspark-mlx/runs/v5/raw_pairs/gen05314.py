from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue").desc())
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "units",
        "revenue",
        F.cume_dist().over(w).alias("revenue_cume_dist"),
        F.sum("revenue").over(w2).alias("cumulative_revenue"),
    )
)
