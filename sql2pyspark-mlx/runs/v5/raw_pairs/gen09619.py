from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        F.sum("units").over(w).alias("running_sum_units"),
        F.min("units").over(w).alias("running_min_units"),
        F.avg("units").over(w).alias("running_avg_units"),
    )
)
