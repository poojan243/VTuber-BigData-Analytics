SELECT 
    c.*  -- Select all columns from the chats table
FROM 
    (
        SELECT * FROM OPENROWSET(
            BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/output/chat1/*.csv',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            FIRSTROW = 1
        ) AS chat1
        UNION ALL
        SELECT * FROM OPENROWSET(
            BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/output/chat2/*.csv',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            FIRSTROW = 1 
        ) AS chat2
        UNION ALL
        SELECT * FROM OPENROWSET(
            BULK 'https://bigdatalakeproj.dfs.core.windows.net/vtuberdata/output/chat3/*.csv',
            FORMAT = 'CSV',
            PARSER_VERSION = '2.0',
            FIRSTROW = 1 
        ) AS chat3
    ) AS c;