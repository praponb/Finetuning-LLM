from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("page_title", F.initcap(F.trim("page")))
    .withColumn("event_type_page", F.concat("event_type", F.lit("/"), "page"))
)
