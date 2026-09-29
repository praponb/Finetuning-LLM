from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "rating",
        F.ntile(3).over(w).alias("bucket"),
        F.lead("rating", 1, 0).over(w2).alias("next_rating"),
    )
)
