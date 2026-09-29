from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")

result = (
    df_store_sales
    .filter(F.col("units") >= 13)
    .select(
        "id",
        "product_id",
        "units",
        F.lag("units").over(w_part).alias("prev_units"),
        F.lag("revenue").over(w_part).alias("prev_revenue"),
        F.row_number().over(w_part).alias("event_seq"),
        F.ntile(4).over(w2).alias("revenue_bucket"),
    )
)
