from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "city",
        F.date_format("opened_date", "yyyy-MM-dd").alias("opened_date_label"),
    )
)
