from pyspark.sql import functions as F

result = (
    df_stores
    .select(
        "id",
        "region",
        "opened_date",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
    .orderBy(F.col("id").desc(), "opened_date")
    .limit(5)
)
