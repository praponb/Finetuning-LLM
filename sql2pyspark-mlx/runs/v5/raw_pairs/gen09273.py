from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("country").orderBy(F.col("rating").asc_nulls_last())
w3 = Window.orderBy("rating", "id")
w4 = Window.partitionBy("country").orderBy("rating", "id")
w5 = Window.orderBy("rating", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.round((F.col("rating") / F.sum("rating").over(w)), 4).alias("rating_share"),
        F.rank().over(w2).alias("rating_rank"),
        F.lead("rating", 2, 0).over(w3).alias("next_rating"),
        F.lag("rating", 2).over(w4).alias("prev_rating"),
        F.avg("rating").over(w5).alias("rating_avg_2"),
    )
)
