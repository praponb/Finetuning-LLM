from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w_running = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("clean_city", F.trim("city"))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

result = (
    filtered
    .withColumn("prev_rating", F.lag("rating", 2).over(w))
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w_running))
)
