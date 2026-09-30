from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("city") == "New York")
    .select(
        "id",
        "city",
        "rating",
        (F.col("rating") - F.lag("rating", 2, 0).over(w)).alias("rating_change"),
    )
    .orderBy("rating_change")
)
