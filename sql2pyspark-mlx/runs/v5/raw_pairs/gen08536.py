from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("product_id").orderBy("units", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        F.lag("units").over(w_ordered).alias("prev_units"),
        F.lag("revenue").over(w_ordered).alias("prev_revenue"),
        F.lead("revenue").over(w_ordered).alias("next_revenue"),
        F.row_number().over(w_ordered).alias("row_seq"),
    )
)
