from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("city").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Trad%"))
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.first("city").over(w).alias("first_city"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
    )
)
