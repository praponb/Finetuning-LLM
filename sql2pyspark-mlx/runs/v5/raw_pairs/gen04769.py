from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        F.lead("units").over(w).alias("next_units"),
        F.lag("units").over(w).alias("prev_units"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("units").over(w2).alias("product_id_sum_units"),
    )
)
