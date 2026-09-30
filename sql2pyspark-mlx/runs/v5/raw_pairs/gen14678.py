from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "store_id",
        "sale_date",
        "revenue",
        "units",
        F.lag("revenue", 2).over(w).alias("prev_revenue"),
        F.sum("units").over(w2).alias("running_units"),
    )
    .orderBy(F.col("revenue").desc())
)
