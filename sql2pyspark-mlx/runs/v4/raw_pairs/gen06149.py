from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id")

result = (
    df_store_sales
    .select(
        "*",
        F.lead("revenue", 1).over(w_part).alias("next_revenue"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
    )
)
