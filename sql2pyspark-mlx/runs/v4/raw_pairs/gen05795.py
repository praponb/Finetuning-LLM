from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.sum("rating").over(w2).alias("country_sum_rating"),
    )
    .orderBy(F.col("next_rating").desc())
)
