-- 1. Création de la table brute
CREATE TABLE crypto_raw (
    SNo INT,
    Name VARCHAR(100),
    Symbol VARCHAR(20),
    Date_Time TIMESTAMP,
    High NUMERIC,
    Low NUMERIC,
    Open NUMERIC,
    Close NUMERIC,
    Volume NUMERIC,
    Marketcap NUMERIC
);


CREATE VIEW view_crypto_cleaned AS
SELECT 
    -- Conversion de la date (suppression de l'heure si inutile pour l'analyse journalière)
    CAST(Date_Time AS DATE) as Date_Jour,
    Symbol,
    Name,
    Open,
    High,
    Low,
    Close,
    Volume,
    Marketcap
FROM crypto_raw
WHERE 
    -- Suppression des lignes où les informations critiques sont manquantes
    Name IS NOT NULL 
    AND Date_Time IS NOT NULL
    AND Close IS NOT NULL
    -- Suppression des incohérences logiques (ex: High ne peut pas être inférieur à Low)
    AND High >= Low;

CREATE VIEW view_crypto_enriched AS
SELECT 
    *,
    -- 1. Range High-Low (Écart absolu journalier)
    (High - Low) AS Range_High_Low,
    
    -- 2. Moyenne High/Low (Prix moyen journalier estimé)
    ((High + Low) / 2) AS Avg_High_Low,
    
    -- 3. Volatilité Journalière en % (Spread)
    CASE WHEN Open = 0 THEN 0 
         ELSE ((High - Low) / Open) * 100 
    END AS Volatility_Pct,
    
    -- 4. Variation Journalière (Pour voir si le marché est haussier ou baissier ce jour-là)
    (Close - Open) AS Daily_Change
FROM view_crypto_cleaned;

CREATE VIEW view_crypto_trends AS
SELECT 
    Date_Jour,
    Symbol,
    Close,
    -- Moyenne Mobile sur 7 jours (Court terme)
    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date_Jour ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as MA_7,
    -- Moyenne Mobile sur 30 jours (Moyen terme)
    AVG(Close) OVER (PARTITION BY Symbol ORDER BY Date_Jour ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as MA_30
FROM view_crypto_cleaned;

CREATE VIEW view_crypto_kpi_yearly AS
SELECT 
    Symbol,
    EXTRACT(YEAR FROM Date_Jour) AS Annee,
    -- Prix Moyen sur l'année
    ROUND(AVG(Close), 4) AS Prix_Moyen_Annuel,
    -- Prix Max atteint (ATH annuel)
    MAX(High) AS Prix_Max_Annuel,
    -- Volatilité (Écart type du prix de clôture)
    ROUND(STDDEV(Close), 4) AS Volatilite_Standard_Deviation,
    -- Volume total échangé
    SUM(Volume) AS Volume_Total
FROM view_crypto_cleaned
GROUP BY Symbol, EXTRACT(YEAR FROM Date_Jour)
ORDER BY Symbol, Annee;