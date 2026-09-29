from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("country")
w4 = Window.orderBy("rating", "id").rowsBetween(-6, -1)

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat("city", F.lit(" "), "supplier_name").alias("city_supplier_name"),
        F.regexp_replace("supplier_name", " ", "").alias("supplier_name_slug"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    valid
    .filter(F.col("country") == "DE")
    .select(
        "*",
        F.ntile(3).over(w).alias("rating_bucket"),
        F.row_number().over(w2).alias("row_seq"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
        F.avg("rating").over(w4).alias("rating_avg_6_prior"),
    )
)
