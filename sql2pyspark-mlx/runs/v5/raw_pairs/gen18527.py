from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("country")
w3 = Window.orderBy("rating", "id").rowsBetween(-3, -1)

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "rating",
        F.percent_rank().over(w).alias("rating_percent_rank"),
        F.max("rating").over(w2).alias("country_max_rating"),
        F.avg("rating").over(w3).alias("rating_avg_3_prior"),
    )
)
