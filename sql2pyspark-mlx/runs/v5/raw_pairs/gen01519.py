from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country", "city").orderBy("rating", "id")

base = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.col("rating").cast("int").alias("rating_int"),
        F.concat("city", F.lit(" "), "supplier_name").alias("city_supplier_name"),
    )
)

result = (
    base
    .filter(~F.col("city").isin(["London", "Tokyo", "Singapore"]))
    .select(
        "id",
        "country",
        "city",
        "rating_int",
        "city_supplier_name",
        F.lag("rating_int").over(w_grp).alias("prev_rating_int"),
        F.lead("rating").over(w_grp).alias("next_rating"),
        F.row_number().over(w_grp).alias("event_seq"),
    )
)
