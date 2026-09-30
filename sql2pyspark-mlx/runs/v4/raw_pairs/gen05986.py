from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("starts_on").desc(), "id")
w2 = Window.orderBy(F.col("ends_on").desc(), "id")

valid = (
    df_promotions
    .filter(F.col("ends_on") > "2023-03-15")
    .select(
        "id",
        "starts_on",
        "ends_on",
        F.upper("promo_code").alias("promo_code_upper"),
    )
)

result = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("seq_no"),
    )
)
