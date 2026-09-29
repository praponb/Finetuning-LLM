from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("rating") <= 2.2)
    .select(
        "id",
        "rating",
        (F.col("rating") - F.lag("rating", 1).over(w)).alias("rating_change"),
    )
    .orderBy(F.col("rating_change").desc(), "rating")
)
