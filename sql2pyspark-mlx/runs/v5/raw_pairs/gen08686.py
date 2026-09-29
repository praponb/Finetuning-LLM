from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("city").orderBy("rating", "id")
w4 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-2, -1)

scored = (
    df_suppliers
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("rating_avg_2_prior", F.avg("rating").over(w4))
)

result = (
    scored
    .select(
        "id",
        "city",
        "rating",
        "last_known_city",
        "seq_no",
        "row_seq",
        "rating_avg_2_prior",
    )
)
