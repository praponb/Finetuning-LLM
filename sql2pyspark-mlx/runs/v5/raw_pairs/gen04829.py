from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id")

result = (
    df_suppliers
    .withColumn("prev_rating", F.lag("rating").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)
