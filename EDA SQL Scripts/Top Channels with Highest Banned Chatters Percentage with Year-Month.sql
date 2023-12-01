-- EDA: Top Channels with Highest Banned Chatters Percentage with Period
SELECT TOP 10
    channelId,
    englishName,
    affiliation,
    LEFT(period, 7) AS Period,
    (CAST(SUM(bannedChatters) AS FLOAT) / NULLIF(SUM(chats), 0)) * 100 AS bannedChattersPercentage
FROM
    OPENROWSET(
        BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/final/chat_stats_channel/chat_stats_channel.csv',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        FIELDTERMINATOR = ',',
        FIRSTROW = 2,
        CODEPAGE = '65001' -- UTF-8 code page
    ) WITH (
        channelId VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        period VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        chats INT,
        memberChats INT,
        uniqueChatters INT,
        uniqueMembers INT,
        bannedChatters INT,
        deletedChats INT,
        chatStatsId INT,
        englishName VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS,
        affiliation VARCHAR(255) COLLATE SQL_Latin1_General_CP1_CI_AS
    ) AS [result]
GROUP BY
    channelId, englishName, affiliation, LEFT(period, 7)
ORDER BY
    bannedChattersPercentage DESC;
