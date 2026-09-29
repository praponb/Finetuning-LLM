from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.round(F.avg("rating").over(w2), 2).alias("rating_moving_avg_4"),
    )
    .orderBy(F.col("rating_moving_avg_4").desc())
)
