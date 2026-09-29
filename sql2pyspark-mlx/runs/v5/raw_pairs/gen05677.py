from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.lead("units").over(w).alias("next_units"),
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
    )
)
