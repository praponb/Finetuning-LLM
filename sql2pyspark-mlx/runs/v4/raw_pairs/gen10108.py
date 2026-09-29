from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "sale_date",
        "revenue",
        F.avg("units").over(w).alias("product_id_avg_units"),
        F.sum("revenue").over(w2).alias("running_revenue"),
    )
)
