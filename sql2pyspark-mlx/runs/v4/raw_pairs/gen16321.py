from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("rating").desc(), "id")
w3 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "*",
        F.sum("rating").over(w).alias("cumulative_rating"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)
