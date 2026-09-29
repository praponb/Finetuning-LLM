from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
        F.avg("rating").over(w2).alias("rating_avg_3"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
