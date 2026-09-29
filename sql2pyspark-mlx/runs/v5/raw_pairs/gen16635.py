from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.lag("rating", 1).over(w).alias("prev_rating"),
        F.row_number().over(w2).alias("rating_row"),
    )
)
