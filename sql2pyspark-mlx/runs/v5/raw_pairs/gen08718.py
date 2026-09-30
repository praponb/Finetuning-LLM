from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("age")
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "city",
        "age",
        "country",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.min("age").over(w2).alias("country_min_age"),
    )
    .orderBy(F.col("city").desc())
)
