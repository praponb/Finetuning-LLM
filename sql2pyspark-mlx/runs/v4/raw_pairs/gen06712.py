from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("total_rating").desc())

result = (
    df_suppliers
    .groupBy(
        F.when(F.col("city").isin(["Chiang Mai", "New York"]), "core").otherwise("other").alias("city_group"),
    )
    .agg(F.sum("rating").alias("total_rating"))
    .withColumn("rating_rank", F.rank().over(w))
    .orderBy(F.col("city_group").desc(), F.col("total_rating").desc())
)
