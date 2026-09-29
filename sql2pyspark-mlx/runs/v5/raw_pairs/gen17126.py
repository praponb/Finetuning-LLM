from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")
w_ordered = Window.orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc(), "id")

filtered = (
    df_sessions
    .filter(F.col("device").isin(["tablet", "desktop", "mobile"]))
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.upper("country").alias("country_upper"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "customer_id",
        "started_at",
        F.row_number().over(w).alias("row_seq"),
        F.first("country", ignorenulls=True).over(w_ordered).alias("first_known_country"),
        F.first("country", ignorenulls=True).over(w_running).alias("first_known_country_2"),
        F.first("country", ignorenulls=True).over(w_running).alias("first_known_country_3"),
        F.row_number().over(w4).alias("row_seq_2"),
    )
)
