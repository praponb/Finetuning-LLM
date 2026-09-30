from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id", "attr_key")

flattened = (
    df_carts
    .select("id", "created_at", F.explode("attributes").alias("attr_key", "value"))
)

result = (
    flattened
    .select("id", "created_at", "attr_key", F.row_number().over(w).alias("row_seq"))
)
