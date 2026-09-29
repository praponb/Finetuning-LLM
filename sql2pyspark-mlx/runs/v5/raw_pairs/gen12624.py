from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.row_number().over(w).alias("row_num"),
        F.sum("rating").over(w2).alias("city_sum_rating"),
    )
    .orderBy(F.col("row_num").desc())
)
