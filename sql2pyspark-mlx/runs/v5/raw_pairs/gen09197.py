from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Parts%"))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.max("rating").over(w).alias("country_max_rating"),
    )
    .orderBy("city")
)
