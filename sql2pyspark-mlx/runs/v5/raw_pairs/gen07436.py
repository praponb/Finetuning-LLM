from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("T%"))
    .select("id", "rating", F.lead("rating", 1, 0).over(w).alias("next_rating"))
)
