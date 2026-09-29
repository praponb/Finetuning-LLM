from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment")

result = (
    df_customers
    .select(
        "id",
        "country",
        "segment",
        "age",
        F.round((F.col("age") / F.sum("age").over(w)), 3).alias("age_share"),
    )
)
