from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_suppliers
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w)), 4))
    .withColumn("first_known_rating", F.first("rating", ignorenulls=True).over(w2))
    .withColumn("running_rating", F.sum("rating").over(w3))
)

result = (
    enriched
    .select("id", "supplier_name", "country", "city", "rating", "rating_share")
    .orderBy(F.col("country").desc_nulls_first())
)
