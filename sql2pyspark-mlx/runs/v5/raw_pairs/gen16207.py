from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Singapore", "New York"]))
    .withColumn("cumulative_rating", F.sum("rating").over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)
