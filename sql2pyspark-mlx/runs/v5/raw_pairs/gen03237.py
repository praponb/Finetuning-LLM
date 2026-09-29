from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("city") != "London")
    .select(
        "id",
        "rating",
        F.max("rating").over(w_part).alias("last4_max_rating"),
        F.min("rating").over(w_part).alias("last4_min_rating"),
        F.sum("rating").over(w_part).alias("last4_sum_rating"),
    )
)
