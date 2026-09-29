from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.min("rating").over(w).alias("city_min_rating"),
    )
)
