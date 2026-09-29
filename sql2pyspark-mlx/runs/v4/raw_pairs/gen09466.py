from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

gaps = (
    df_sessions
    .withColumn("new_group_flag", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

islands = gaps.withColumn("island_id", F.sum("new_group_flag").over(w2))

result = (
    islands
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        "island_id",
        F.when(F.col("country").rlike("^(SG|TH)$"), "CORE").when(F.col("country").rlike("^(US|JP)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
