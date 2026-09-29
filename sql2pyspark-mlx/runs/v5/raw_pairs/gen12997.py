from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("revenue").desc(), "id")
w2 = Window.partitionBy("product_id")
w_ordered = Window.partitionBy("product_id").orderBy("sale_date", "id")

result = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.row_number().over(w).alias("seq_no"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
        F.lag("revenue", 1).over(w_ordered).alias("prev_revenue"),
    )
)
