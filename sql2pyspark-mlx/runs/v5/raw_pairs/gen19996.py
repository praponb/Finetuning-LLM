from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("To%"))
    .select(
        "id",
        "country",
        "rating",
        F.max("rating").over(w).alias("country_max_rating"),
    )
    .orderBy("country", "country_max_rating")
)
