from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "customer_id").orderBy("is_safari", "id")
w2 = Window.partitionBy("customer_id").orderBy("is_safari", "id")

filtered = (
    df_sessions
    .filter((F.col("started_at") >= F.lit("2023-05-20").cast("date")) & F.col("country").isNull())
    .select(
        "id",
        "customer_id",
        "device",
        "started_at",
        "country",
        F.when(F.col("browser") == "Safari", 1).otherwise(0).alias("is_safari"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_clean"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.lead("is_safari", 2).over(w2).alias("next_is_safari"),
    )
)
