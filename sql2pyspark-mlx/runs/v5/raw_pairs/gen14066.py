from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment").orderBy(F.col("age").desc())

result = (
    df_customers
    .select(
        "id",
        "country",
        "age",
        "segment",
        F.max("age").over(w).alias("country_max_age"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("age").desc())
)
