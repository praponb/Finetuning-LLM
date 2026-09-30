from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc())
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.rank().over(w).alias("rating_rank"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 3).alias("rating_share"),
    )
)
