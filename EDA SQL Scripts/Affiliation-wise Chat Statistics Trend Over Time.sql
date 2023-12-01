-- EDA: Affiliation-wise Chat Statistics Trend Over Time
SELECT
    affiliation,
    LEFT(period, 7) AS yearMonth,
    AVG(chats) AS avgChats,
    AVG(memberChats) AS avgMemberChats,
    AVG(uniqueChatters) AS avgUniqueChatters,
    AVG(uniqueMembers) AS avgUniqueMembers
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
    affiliation, LEFT(period, 7)
ORDER BY
    affiliation, LEFT(period, 7);
