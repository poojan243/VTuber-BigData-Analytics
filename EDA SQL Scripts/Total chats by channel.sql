-- Query to get total chats for top 10 channels
SELECT TOP 10
    channelId,
    SUM(chats) AS totalChats
FROM
    OPENROWSET(
        BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/output/chat_stats/chat_stats.csv',
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
        chatStatsId INT
    ) AS [result]
GROUP BY
    channelId
ORDER BY
    totalChats DESC;