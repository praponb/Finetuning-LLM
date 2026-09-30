from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w_recent = Window.partitionBy("customer_id", "country").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "customer_id").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("country").isin(["DE", "SG", "UK"]))
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
        F.row_number().over(w4).alias("seq_no_2"),
    )
)
