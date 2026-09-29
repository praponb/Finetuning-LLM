from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-6, -1)
w2 = Window.orderBy(F.col("rating").desc_nulls_first())
w3 = Window.partitionBy("city", "country").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        F.avg("rating").over(w).alias("rating_avg_6_prior"),
        F.percent_rank().over(w2).alias("rating_percent_rank"),
        F.ntile(3).over(w3).alias("rating_bucket"),
    )
)
