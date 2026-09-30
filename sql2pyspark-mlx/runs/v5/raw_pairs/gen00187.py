from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("rating", "id")
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Globa%"))
    .select(
        "id",
        "city",
        "rating",
        F.lead("rating", 1).over(w_recent).alias("next_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w2)), 4).alias("rating_share"),
        F.lag("rating", 1, 0).over(w_recent).alias("prev_rating"),
    )
)
