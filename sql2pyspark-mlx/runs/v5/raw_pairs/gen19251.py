from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy(F.col("rating").asc_nulls_first())
w5 = Window.orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        F.lead("rating", 1).over(w).alias("next_rating"),
        F.sum("rating").over(w2).alias("running_rating"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
        F.rank().over(w4).alias("rating_rank"),
        F.row_number().over(w5).alias("seq_no"),
    )
)
