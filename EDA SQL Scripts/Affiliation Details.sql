-- Query to get details for the top 10 affiliations
WITH TopAffiliations AS (
    SELECT TOP 10
        affiliation,
        COUNT(*) AS total_channels,
        AVG(CONVERT(INT, subscriptionCount)) AS avg_subscribers,
        MAX(CONVERT(INT, subscriptionCount)) AS max_subscribers,
        MIN(CONVERT(INT, subscriptionCount)) AS min_subscribers,
        SUM(CONVERT(INT, subscriptionCount)) AS total_subscribers
    FROM
        OPENROWSET(
            BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/output/channel/channels.csv',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            FIELDTERMINATOR = ',',
            FIRSTROW = 2,
            CODEPAGE = '65001' -- UTF-8 code page
        ) WITH (
            channelId VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
            englishName VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
            affiliation VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
            subscriptionCount VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
            videoCount VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS
        ) AS [result]
    GROUP BY
        affiliation
    ORDER BY
        total_subscribers DESC
)
SELECT
    affiliation,
    total_channels,
    avg_subscribers,
    max_subscribers,
    min_subscribers,
    total_subscribers
FROM
    TopAffiliations
ORDER BY
    avg_subscribers DESC;
