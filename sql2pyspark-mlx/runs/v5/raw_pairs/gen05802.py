from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("brand", "category").orderBy("cost", "id").rowsBetween(-3, Window.currentRow)
w_grp = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("category", "supplier_id").orderBy("cost", "id")

result = (
    df_products
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "launch_date",
        "supplier_id",
        F.avg("price").over(w_hist).alias("price_avg_3"),
        F.sum("price").over(w_grp).alias("running_price"),
        F.ntile(10).over(w_part).alias("cost_bucket"),
    )
)
