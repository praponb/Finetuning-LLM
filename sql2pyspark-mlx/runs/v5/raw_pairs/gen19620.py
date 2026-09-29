from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        (F.col("rating") - F.lag("rating", 1, 0).over(w)).alias("rating_change"),
        F.lead("rating", 1, 0).over(w2).alias("next_rating"),
    )
)
