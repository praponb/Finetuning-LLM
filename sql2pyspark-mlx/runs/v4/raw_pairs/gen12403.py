from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "country",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.max("rating").over(w2).alias("country_max_rating"),
    )
    .orderBy("country_max_rating")
)
