from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
    )
)
