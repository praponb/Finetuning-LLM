from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.row_number().over(w).alias("row_num"),
        F.row_number().over(w2).alias("row_num_2"),
    )
)
