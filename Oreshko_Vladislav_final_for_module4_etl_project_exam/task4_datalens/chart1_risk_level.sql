SELECT 
    risk_level,
    COUNT(*) AS total_applications,
    SUM(CAST(is_approved AS Int32)) AS approved
FROM `etl-exam-source`.`processed/loan_applications/event_year=2026/`
WITH (FORMAT="parquet", SCHEMA=(
    risk_level Utf8,
    decision_status Utf8,
    is_approved Bool
))
GROUP BY risk_level
ORDER BY total_applications DESC