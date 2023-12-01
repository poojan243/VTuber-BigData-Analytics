-- EDA: Top 10 Affiliations with highest average body length of chats
SELECT TOP 10
    affiliation,
    AVG(bodyLength) AS avgBodyLength
FROM
    OPENROWSET(
        BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/final/chat_channel_merged/chat_channel_merged.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        CODEPAGE = '65001' -- UTF-8 code page
    ) WITH (
        channelId VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        authorChannelId VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        videoId VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        timestamp VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        isMember VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
        bodyLength INT,
        chatId INT,
        deleted_by_mod VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
        banned VARCHAR(10) COLLATE SQL_Latin1_General_CP1_CI_AS,
        englishName VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        affiliation VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS
    ) AS [result]
GROUP BY
    affiliation
ORDER BY
    avgBodyLength DESC;
