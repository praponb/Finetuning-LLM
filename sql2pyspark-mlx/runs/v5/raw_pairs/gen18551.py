from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.orderBy("sqft")

result = (
    df_stores
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        F.row_number().over(w).alias("rn"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
