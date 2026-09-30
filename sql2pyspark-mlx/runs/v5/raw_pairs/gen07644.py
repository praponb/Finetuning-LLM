from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.min("rating").over(w).alias("country_min_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
    .orderBy(F.col("running_rating").desc())
)
