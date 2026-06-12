SELECT 
    channel,
    CAST(AVG(CAST(credit_score AS Double)) AS Double) AS avg_score,
    COUNT(*) AS total
FROM `etl-exam-source`.`processed/loan_applications/event_year=2026/`
WITH (FORMAT="parquet", SCHEMA=(
    channel Utf8,
    credit_score Int32
))
GROUP BY channel
ORDER BY avg_score DESC