from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("started_at").desc(), "id")
w2 = Window.partitionBy("device").orderBy(F.col("started_at").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id")

scored = (
    df_sessions
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        "event_seq",
        F.when(F.col("country").rlike("^(JP|DE)$"), "GROUP_A").when(F.col("country").rlike("^(TH|US)$"), "PRIORITY").otherwise("EXTENDED").alias("country_zone"),
    )
)
