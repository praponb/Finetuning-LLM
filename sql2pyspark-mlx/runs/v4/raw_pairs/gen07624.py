from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "*",
        F.lead("units", 1).over(w).alias("next_units"),
        F.min("revenue").over(w2).alias("product_id_min_revenue"),
    )
)
