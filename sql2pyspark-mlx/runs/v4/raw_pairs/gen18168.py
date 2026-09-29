from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.orderBy(F.col("rating").desc(), "id")

enriched = (
    df_suppliers
    .withColumn("last_known_rating", F.last("rating", ignorenulls=True).over(w))
    .withColumn("city_max_rating", F.max("rating").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    enriched
    .select("id", "supplier_name", "rating", "city_max_rating", "seq_no")
    .orderBy(F.col("rating").desc())
)
