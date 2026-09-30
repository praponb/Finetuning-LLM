from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id")
w2 = Window.orderBy("rating", "id")

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("rating") > 3.1))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.concat("country", F.lit(" - "), "city").alias("country_city"),
    )
)

result = (
    filtered
    .withColumn("prev_rating", F.lag("rating", 1).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)
