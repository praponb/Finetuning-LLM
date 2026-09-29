from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units_whole").desc(), "id")
w_running = Window.orderBy("units", "id")

base = (
    df_store_sales
    .filter(F.col("revenue") > 3300)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.floor("units").alias("units_whole"),
    )
)

result = (
    base
    .select(
        "*",
        F.ntile(5).over(w).alias("units_whole_bucket"),
        F.lead("revenue", 1).over(w_running).alias("next_revenue"),
    )
)
