from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("rating", "id")

result = (
    df_suppliers
    .filter(F.length("supplier_name") < 5)
    .select("id", "country", "rating", F.sum("rating").over(w).alias("running_rating"))
)
