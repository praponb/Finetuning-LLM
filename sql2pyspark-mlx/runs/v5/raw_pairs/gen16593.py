from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .select(
        "id",
        "country",
        "rating",
        "city",
        F.first("city").over(w).alias("first_city"),
    )
)
