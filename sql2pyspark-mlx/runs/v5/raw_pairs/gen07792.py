from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-6, -1)

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Electronics"]))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.last("brand", ignorenulls=True).over(w2).alias("last_known_brand"),
        F.avg("stock_qty").over(w3).alias("stock_qty_avg_2"),
        F.avg("price").over(w4).alias("price_avg_6_prior"),
    )
)
