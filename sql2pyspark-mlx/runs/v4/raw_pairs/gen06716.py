from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_ordered = Window.orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "*",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.ntile(3).over(w_ordered).alias("rating_bucket"),
        F.sum("rating").over(w3).alias("city_sum_rating"),
        F.row_number().over(w_ordered).alias("row_seq"),
        F.lag("rating", 2).over(w4).alias("prev_rating"),
    )
)
