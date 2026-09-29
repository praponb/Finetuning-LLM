from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("rating") > 2.8)
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.min("rating").over(w).alias("city_min_rating"),
        F.dense_rank().over(w2).alias("rating_dense_rank"),
    )
)
