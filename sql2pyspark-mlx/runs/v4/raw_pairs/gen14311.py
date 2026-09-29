from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc())
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")

result = (
    df_stores
    .select(
        "id",
        "opened_date",
        "city",
        "sqft",
        F.dense_rank().over(w).alias("opened_date_dense_rank"),
        F.row_number().over(w2).alias("rn"),
    )
)
