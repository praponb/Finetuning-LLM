from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.sum("rating").over(w).alias("running_rating"),
        F.sum("rating").over(w2).alias("city_sum_rating"),
    )
    .orderBy("rating")
)
