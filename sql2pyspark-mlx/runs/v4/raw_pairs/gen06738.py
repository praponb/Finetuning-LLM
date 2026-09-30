from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_suppliers
    .select(
        "*",
        F.lead("rating", 2).over(w).alias("next_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
        F.row_number().over(w3).alias("event_seq"),
        F.avg("rating").over(w4).alias("rating_avg_3"),
        F.sum("rating").over(w5).alias("cumulative_rating"),
    )
)
