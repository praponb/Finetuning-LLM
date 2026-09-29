from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "rating",
        "city",
        (F.col("rating") - F.lag("rating", 1).over(w)).alias("rating_change"),
        F.rank().over(w2).alias("rating_rank"),
    )
    .orderBy(F.col("rating_change").desc())
)
