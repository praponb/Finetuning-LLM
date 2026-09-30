from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.orderBy("units", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .filter(F.col("revenue").between(1900, 3300))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.avg("units").over(w2).alias("units_avg_6"),
        F.sum("units").over(w3).alias("running_units"),
        F.round((F.col("units") / F.sum("units").over(w4)), 4).alias("units_share"),
        F.avg("units").over(w4).alias("product_id_avg_units"),
    )
)
