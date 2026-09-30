from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.row_number().over(w).alias("rn"),
        F.rank().over(w2).alias("rating_rank"),
    )
)
