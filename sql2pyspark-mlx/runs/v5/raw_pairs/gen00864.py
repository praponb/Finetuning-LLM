from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating")
w2 = Window.partitionBy("city")

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.rank().over(w).alias("rnk"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 3).alias("rating_share"),
    )
)
