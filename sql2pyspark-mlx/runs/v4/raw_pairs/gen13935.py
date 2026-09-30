from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("rating").desc())
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("rating_cume_dist").desc_nulls_last(), "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .withColumn("supplier_name_country", F.concat("supplier_name", F.lit("/"), "country"))
)

scored = (
    base
    .select(
        "*",
        F.cume_dist().over(w).alias("rating_cume_dist"),
        F.sum("rating").over(w2).alias("running_rating"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

valid = (
    scored
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.concat("country", F.lit(", "), "city").alias("country_city"),
        F.upper("supplier_name").alias("supplier_name_upper"),
    )
)

latest = (
    valid
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "country",
        "rating",
        "supplier_name_country",
        "rating_cume_dist",
        "seq_no",
        "country_city",
    )
)
