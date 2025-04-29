require('dotenv').config();
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  ScanCommand,
  UpdateCommand,
  QueryCommand
} = require("@aws-sdk/lib-dynamodb");
const { defaultProvider } = require("@aws-sdk/credential-provider-node");

const TABLE_NAME = process.env.DYNAMODB_TABLE || "DiscordAccounts";
const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const ATTRIBUTE_INDEX_NAME = "attributeName-count-index";

let credentialsProvider;
const personalKeyId = process.env.PERSONAL_AWS_ACCESS_KEY_ID;
const personalSecretKey = process.env.PERSONAL_AWS_SECRET_ACCESS_KEY;

if (personalKeyId && personalSecretKey) {
  credentialsProvider = async () => {
    if (!personalKeyId || !personalSecretKey) { throw new Error("PERSONAL AWS keys missing/empty."); }
    if (personalKeyId.length < 16 || personalSecretKey.length < 30) { console.warn("[Credentials] Warning: PERSONAL AWS keys appear short."); }
    return { accessKeyId: personalKeyId, secretAccessKey: personalSecretKey };
  };
} else {
  credentialsProvider = defaultProvider();
}

let ddbClient;
try {
  ddbClient = new DynamoDBClient({ region: AWS_REGION, credentials: credentialsProvider });
} catch (clientInitError) {
  console.error("[Credentials] CRITICAL ERROR initializing DynamoDBClient:", clientInitError);
  process.exit(1);
}

const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

function getNextMidnightTimestamp() {
  const now = new Date();
  const nextMidnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1));
  return Math.floor(nextMidnight.getTime() / 1000);
}

const LEADERBOARD_ATTRIBUTES = [
  'streak', 'messages', 'highestStreak', 'messageLeaderWins',
  'averageMessagesPerDay', 'level',
  'totalXp',
  'activeDaysCount', 'longestInactivePeriod', 'mostConsecutiveLeader'
];

const EXPIRING_ATTRIBUTES = [
  'streak',
  'activeDaysCount'
];

function safeParseNumber(value, defaultValue = 0) {
  if (value === null || value === undefined) {
    return defaultValue;
  }
  if (typeof value === 'number' && !isNaN(value)) {
    return value;
  }
  const parsed = Number(value);
  return isNaN(parsed) ? defaultValue : parsed;
}

function createAttributeItem(guildId, userId, attributeName, rawCount) {
  const count = safeParseNumber(rawCount, null);
  if (count === null || typeof count !== 'number') {
    console.warn(`[DynamoDB] createAttributeItem skipped for ${userId}-${attributeName} due to invalid/non-numeric count: ${rawCount} (Type: ${typeof rawCount})`);
    return null;
  }
  const now = new Date().toISOString();
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const item = {
    DiscordId: `${primaryUserId}-${attributeName}`,
    attributeName: attributeName,
    count: count,
    discordAccountId: primaryUserId,
    guildId: primaryGuildId,
    lastUpdated: now
  };
  if (EXPIRING_ATTRIBUTES.includes(attributeName)) {
    item.expireAt = getNextMidnightTimestamp();
  }
  return item;
}

async function getRawUserData(userId) {
  if (!userId) {
    console.error("[DynamoDB] getRawUserData missing userId.");
    return null;
  }
  const primaryUserId = String(userId);
  try {
    const params = { TableName: TABLE_NAME, Key: { DiscordId: primaryUserId } };
    const { Item } = await ddbDocClient.send(new GetCommand(params));
    return Item || null;
  } catch (error) {
    console.error(`[DynamoDB] Error in getRawUserData for userId ${primaryUserId}:`, error);
    return null;
  }
}

async function getUserData(guildId, userId) {
  const rawItem = await getRawUserData(userId);
  if (rawItem && typeof rawItem.userData === 'object' && rawItem.userData !== null) {
    return rawItem.userData;
  } else if (rawItem && (!rawItem.userData || typeof rawItem.userData !== 'object')) {
    console.warn(`[DynamoDB] getUserData found raw item for ${userId} but userData field is missing or not an object. Raw item keys:`, Object.keys(rawItem));
    return null;
  }
  return null;
}

async function saveUserData(guildId, userId, userData) {
  if (!userId || !guildId || !userData) {
    console.error("[DynamoDB] saveUserData missing required arguments.", { userId: !!userId, guildId: !!guildId, userData: !!userData });
    throw new Error("saveUserData missing required arguments.");
  }
  const now = new Date().toISOString();
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);

  const primaryItem = {
    DiscordId: primaryUserId,
    guildId: primaryGuildId,
    userData: userData,
    lastUpdated: now
  };

  Object.keys(primaryItem).forEach(k => {
    if (primaryItem[k] === undefined) {
      console.warn(`[DynamoDB SaveSanitize] Deleting undefined top-level key '${k}' for ${primaryUserId}`);
      delete primaryItem[k];
    }
  });
  if (primaryItem.userData && typeof primaryItem.userData === 'object') {
    Object.keys(primaryItem.userData).forEach(k => {
      if (primaryItem.userData[k] === undefined) {
        console.warn(`[DynamoDB SaveSanitize] Deleting undefined userData key '${k}' for ${primaryUserId}`);
        delete primaryItem.userData[k];
      }
      if (k === 'experience' && primaryItem.userData.experience && typeof primaryItem.userData.experience === 'object') {
        Object.keys(primaryItem.userData.experience).forEach(ek => {
          if (primaryItem.userData.experience[ek] === undefined) {
            console.warn(`[DynamoDB SaveSanitize] Deleting undefined experience key '${ek}' for ${primaryUserId}`);
            delete primaryItem.userData.experience[ek];
          }
        });
      }
      if (k === 'lastMessage' && primaryItem.userData.lastMessage && typeof primaryItem.userData.lastMessage === 'object') {
        Object.keys(primaryItem.userData.lastMessage).forEach(lk => {
          if (primaryItem.userData.lastMessage[lk] === undefined) {
            console.warn(`[DynamoDB SaveSanitize] Deleting undefined lastMessage key '${lk}' for ${primaryUserId}`);
            delete primaryItem.userData.lastMessage[lk];
          }
        });
      }
      if (k === 'mentionsRepliesCount' && primaryItem.userData.mentionsRepliesCount && typeof primaryItem.userData.mentionsRepliesCount === 'object') {
        Object.keys(primaryItem.userData.mentionsRepliesCount).forEach(mk => {
          if (primaryItem.userData.mentionsRepliesCount[mk] === undefined) {
            console.warn(`[DynamoDB SaveSanitize] Deleting undefined mentionsRepliesCount key '${mk}' for ${primaryUserId}`);
            delete primaryItem.userData.mentionsRepliesCount[mk];
          }
        });
      }
    });
  } else {
    console.error(`[DynamoDB SaveSanitize] userData is missing or not an object for ${primaryUserId} during save!`);
    primaryItem.userData = {};
  }

  const itemsToPut = [primaryItem];
  for (const attrName of LEADERBOARD_ATTRIBUTES) {
    let countValue;
    if (attrName === 'level') {
      countValue = userData.experience?.level;
    } else if (attrName === 'totalXp') {
      countValue = userData.experience?.totalXp;
    } else if (attrName === 'averageMessagesPerDay') {
      countValue = userData[attrName];
    } else {
      countValue = userData[attrName];
    }

    const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
    if (attributeItem) {
      itemsToPut.push(attributeItem);
    }
  }

  const putPromises = itemsToPut.map(item => {
    const params = { TableName: TABLE_NAME, Item: item };
    return ddbDocClient.send(new PutCommand(params)).catch(error => {
      console.error(`[DynamoDB] Error putting item ${item.DiscordId} during saveUserData for ${primaryUserId}:`, error);
      return { success: false, id: item.DiscordId, error: error };
    });
  });

  try {
    const results = await Promise.all(putPromises);
    const failedPuts = results.filter(r => r && r.success === false);
    if (failedPuts.length > 0) {
      console.warn(`[DynamoDB] ${failedPuts.length}/${putPromises.length} puts failed during saveUserData for ${primaryUserId}. Failed IDs: ${failedPuts.map(f=>f.id).join(', ')}`);
      const primaryFailed = failedPuts.some(f => f.id === primaryUserId);
      if (primaryFailed) {
        console.error(`[DynamoDB] CRITICAL: Failed primary item save for ${primaryUserId}.`);
        throw new Error(`Failed primary save for ${primaryUserId}.`);
      }
    }
  } catch (error) {
    console.error(`[DynamoDB] Major error during saveUserData for ${primaryUserId}:`, error);
    throw error;
  }
}

async function updateUserData(guildId, userId, updates) {
  if (!userId || !guildId) {
    console.error("[DynamoDB] updateUserData missing required arguments.", { userId: !!userId, guildId: !!guildId });
    throw new Error("updateUserData missing required arguments.");
  }
  const updateKeys = Object.keys(updates);
  if (updateKeys.length === 0) {
    console.warn(`[DynamoDB] updateUserData called for ${userId} with empty updates object. Skipping.`);
    return;
  }

  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const now = new Date().toISOString();

  let primaryUpdateExpression = "SET ";
  const primaryExpressionAttributeNames = { '#ud': 'userData', '#lu': 'lastUpdated' };
  const primaryExpressionAttributeValues = { ':lu': now };
  const primaryUpdateParts = [];

  for (const key of updateKeys) {
    const keyParts = key.split('.');
    let pathExpression = '#ud';
    let currentPathForValuePlaceholder = '';
    let value = updates[key];

    for (let i = 0; i < keyParts.length; i++) {
      const part = keyParts[i];
      const namePlaceholder = `#attr_${i}_${part.replace(/[^a-zA-Z0-9_]/g, '')}`;
      primaryExpressionAttributeNames[namePlaceholder] = part;
      pathExpression += `.${namePlaceholder}`;
      currentPathForValuePlaceholder += (i > 0 ? '_' : '') + part.replace(/[^a-zA-Z0-9_]/g, '');
    }

    if (value === undefined) {
      console.warn(`[DynamoDB UpdateSanitize] Attempted to set undefined value for key '${key}' for user ${primaryUserId}. Skipping this field.`);
      continue;
    }

    const valuePlaceholder = `:${currentPathForValuePlaceholder}`;
    primaryExpressionAttributeValues[valuePlaceholder] = value;
    primaryUpdateParts.push(`${pathExpression} = ${valuePlaceholder}`);
  }

  if (primaryUpdateParts.length === 0) {
    console.warn(`[DynamoDB UpdateSanitize] No valid update parts remained after sanitizing for user ${primaryUserId}. Only updating lastUpdated.`);
  }

  primaryUpdateParts.push(`#lu = :lu`);
  primaryUpdateExpression += primaryUpdateParts.join(", ");

  const primaryUpdateParams = {
    TableName: TABLE_NAME,
    Key: { DiscordId: primaryUserId },
    UpdateExpression: primaryUpdateExpression,
    ExpressionAttributeNames: primaryExpressionAttributeNames,
    ExpressionAttributeValues: primaryExpressionAttributeValues,
    ReturnValues: "NONE"
  };

  const attributePutPromises = [];
  for (const key of updateKeys) {
    let attrName = key;
    let countValue = updates[key];

    if (key.startsWith('experience.')) {
      attrName = key.split('.')[1];
    }

    if (LEADERBOARD_ATTRIBUTES.includes(attrName)) {
      const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
      if (attributeItem) {
        attributePutPromises.push(
            ddbDocClient.send(new PutCommand({ TableName: TABLE_NAME, Item: attributeItem })).catch(error => {
              console.error(`[DynamoDB] Error putting attribute ${attrName} during updateUserData for ${primaryUserId}:`, error);
              return { success: false, id: `${primaryUserId}-${attrName}`, error: error };
            })
        );
      }
    }
  }

  try {
    await ddbDocClient.send(new UpdateCommand(primaryUpdateParams));
    if (attributePutPromises.length > 0) {
      const results = await Promise.all(attributePutPromises);
      const failedPuts = results.filter(r => r && r.success === false);
      if (failedPuts.length > 0) {
        console.warn(`[DynamoDB] ${failedPuts.length}/${attributePutPromises.length} attribute puts failed during updateUserData for ${primaryUserId}. Failed IDs: ${failedPuts.map(f=>f.id).join(', ')}`);
      }
    }
  } catch (error) {
    console.error(`[DynamoDB] Error updating user data for userId ${primaryUserId}:`, error);
    if (error.name === 'ValidationException') {
      console.error("[DynamoDB Validation Debug] Expression:", primaryUpdateParams.UpdateExpression);
      console.error("[DynamoDB Validation Debug] Names:", JSON.stringify(primaryUpdateParams.ExpressionAttributeNames));
      console.error("[DynamoDB Validation Debug] Values:", JSON.stringify(primaryUpdateParams.ExpressionAttributeValues));
    }
    throw error;
  }
}

async function listUserData(guildId) {
  if (!guildId) {
    console.error("[DynamoDB] listUserData missing guildId.");
    throw new Error("listUserData missing guildId.");
  }
  const primaryGuildId = String(guildId);
  const params = {
    TableName: TABLE_NAME,
    FilterExpression: "#gid = :gid AND attribute_not_exists(attributeName)",
    ExpressionAttributeNames: { "#gid": "guildId" },
    ExpressionAttributeValues: { ":gid": primaryGuildId },
    ProjectionExpression: "DiscordId, userData"
  };

  let allItems = [];
  let lastEvaluatedKey = null;

  try {
    do {
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }
      const data = await ddbDocClient.send(new ScanCommand(params));
      if (data.Items) {
        allItems = allItems.concat(data.Items);
      }
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (lastEvaluatedKey);

    return allItems.filter(item => {
      if (!item.DiscordId) {
        console.warn(`[DynamoDB listUserData] Filtered out item with missing DiscordId in guild ${primaryGuildId}. Item:`, JSON.stringify(item));
        return false;
      }
      if (!item.userData || typeof item.userData !== 'object') {
        console.warn(`[DynamoDB listUserData] Filtered out item for user ${item.DiscordId} with missing or invalid userData field in guild ${primaryGuildId}. Keys:`, Object.keys(item));
        return false;
      }
      return true;
    }).map(item => ({
      userId: item.DiscordId,
      userData: item.userData
    }));

  } catch (error) {
    console.error(`[DynamoDB] Error listing primary user data for guildId ${primaryGuildId}:`, error);
    throw error;
  }
}

async function incrementMessageLeaderWins(guildId, userId) {
  if (!userId || !guildId) {
    console.error("[DynamoDB] incrementMessageLeaderWins missing required arguments.", { userId: !!userId, guildId: !!guildId });
    throw new Error("incrementMessageLeaderWins missing required arguments.");
  }
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  try {
    const currentData = await getUserData(primaryGuildId, primaryUserId);
    if (!currentData) {
      console.warn(`[DynamoDB] incrementMessageLeaderWins: No user data found for ${primaryUserId} in guild ${primaryGuildId}. Initializing wins to 1.`);
      await updateUserData(primaryGuildId, primaryUserId, { messageLeaderWins: 1 });
    } else {
      const currentWins = safeParseNumber(currentData.messageLeaderWins, 0);
      await updateUserData(primaryGuildId, primaryUserId, { messageLeaderWins: currentWins + 1 });
    }
  } catch (error) {
    console.error(`[DynamoDB] Failed during incrementMessageLeaderWins for userId ${primaryUserId} in guild ${primaryGuildId}:`, error);
    throw error;
  }
}

async function queryLeaderboard(attributeName, guildId, limit = 10) {
  if (!attributeName || !guildId) {
    console.error("[DynamoDB] queryLeaderboard missing required arguments.", { attributeName: !!attributeName, guildId: !!guildId });
    return [];
  }
  if (!LEADERBOARD_ATTRIBUTES.includes(attributeName)) {
    console.error(`[DynamoDB] queryLeaderboard called with invalid attribute name: ${attributeName}`);
    return [];
  }
  const primaryGuildId = String(guildId);
  try {
    const params = {
      TableName: TABLE_NAME,
      IndexName: ATTRIBUTE_INDEX_NAME,
      KeyConditionExpression: '#attrName = :attrNameVal',
      FilterExpression: '#gid = :gidVal',
      ExpressionAttributeNames: {
        '#attrName': 'attributeName',
        '#gid': 'guildId'
      },
      ExpressionAttributeValues: {
        ':attrNameVal': attributeName,
        ':gidVal': primaryGuildId
      },
      ScanIndexForward: false,
      Limit: Math.max(1, Math.min(limit, 50))
    };

    const { Items } = await ddbDocClient.send(new QueryCommand(params));
    return Items || [];
  } catch (error) {
    console.error(`[DynamoDB] Error querying leaderboard for attr "${attributeName}" guild ${primaryGuildId}:`, error);
    if (error.name === 'ResourceNotFoundException') {
      console.error(`[DynamoDB] GSI "${ATTRIBUTE_INDEX_NAME}" not found on table ${TABLE_NAME}. Ensure it is created and active.`);
    } else if (error.name === 'ValidationException' && (error.message.includes('NUMBER') || error.message.includes('Number'))) {
      console.error(`[DynamoDB] GSI "${ATTRIBUTE_INDEX_NAME}" sort key 'count' MUST be Number type! Check data for attribute '${attributeName}'. Query Params:`, JSON.stringify(params));
    } else if (error.name === 'ValidationException') {
      console.error(`[DynamoDB] Query Validation Error: ${error.message}. Query Params:`, JSON.stringify(params));
    }
    return [];
  }
}

module.exports = {
  getUserData,
  getRawUserData,
  saveUserData,
  updateUserData,
  listUserData,
  incrementMessageLeaderWins,
  queryLeaderboard,
  safeParseNumber
};