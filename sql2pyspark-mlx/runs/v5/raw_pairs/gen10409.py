from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Gmb%"))
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.row_number().over(w).alias("rating_row"),
        F.round(F.avg("rating").over(w2), 2).alias("rating_moving_avg_4"),
    )
)
