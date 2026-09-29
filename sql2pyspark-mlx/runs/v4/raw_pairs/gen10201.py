from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc())

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select("id", "rating", F.percent_rank().over(w).alias("rating_pct_rank"))
)
