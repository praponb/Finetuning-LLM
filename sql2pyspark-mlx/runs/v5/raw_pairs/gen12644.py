from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_products
    .select(
        "id",
        "product_name",
        "brand",
        "stock_qty",
        "tags",
        "supplier_id",
        F.avg("cost").over(w).alias("cost_avg_2_prior"),
        F.last("brand", ignorenulls=True).over(w2).alias("last_known_brand"),
        F.first("brand", ignorenulls=True).over(w3).alias("first_known_brand"),
    )
)
