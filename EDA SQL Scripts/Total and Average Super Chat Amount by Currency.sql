-- EDA: Total and Average Super Chat Amount by Currency
SELECT
    mostFrequentCurrency AS currency,
    SUM(totalSC) AS totalSuperChatAmount,
    AVG(averageSC) AS averageSuperChatAmount
FROM
    OPENROWSET(
        BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/output/superchat_stats/superchat_stats.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        CODEPAGE = '65001' -- UTF-8 code page
    ) WITH (
        channelId VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        period VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        superChats INT,
        uniqueSuperChatters INT,
        totalSC INT,
        averageSC INT,
        totalMessageLength INT,
        averageMessageLength INT,
        mostFrequentCurrency VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
        mostFrequentColor VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
        superChatStatsId INT
    ) AS [result]
GROUP BY
    mostFrequentCurrency
ORDER BY
    totalSuperChatAmount DESC;
