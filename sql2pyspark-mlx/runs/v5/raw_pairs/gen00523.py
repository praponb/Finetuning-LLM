from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("city").orderBy("rating", "id")

metrics = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "city",
        F.lag("rating").over(w_part).alias("prev_rating"),
        F.lead("rating").over(w_part).alias("next_rating"),
        F.row_number().over(w_part).alias("event_seq"),
    )
)

result = (
    metrics
    .select(
        "id",
        "supplier_name",
        "city",
        "prev_rating",
        "next_rating",
        "event_seq",
        F.when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "PRIORITY").when(F.col("city").rlike("^(New York|Singapore)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("city_zone"),
    )
)
