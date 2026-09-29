from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
        F.avg("rating").over(w2).alias("country_avg_rating"),
    )
)
