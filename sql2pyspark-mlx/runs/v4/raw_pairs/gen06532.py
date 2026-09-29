from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "*",
        F.last("rating", ignorenulls=True).over(w).alias("last_known_rating"),
        F.ntile(10).over(w2).alias("rating_bucket"),
    )
)
