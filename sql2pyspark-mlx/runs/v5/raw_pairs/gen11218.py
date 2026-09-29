from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        F.sum("revenue").over(w).alias("running_revenue"),
    )
)
