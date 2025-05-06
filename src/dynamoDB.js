require('dotenv').config();
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  ScanCommand,
  UpdateCommand,
  QueryCommand,
  BatchWriteCommand
} = require("@aws-sdk/lib-dynamodb");
const { defaultProvider } = require("@aws-sdk/credential-provider-node");
const { unmarshall } = require("@aws-sdk/util-dynamodb");

const TABLE_NAME = process.env.DYNAMODB_TABLE || "DiscordAccounts";
const AWS_REGION = process.env.AWS_REGION || "us-east-1";
const ATTRIBUTE_INDEX_NAME = "attributeName-count-index";

let credentialsProvider;
const personalKeyId = process.env.PERSONAL_AWS_ACCESS_KEY_ID;
const personalSecretKey = process.env.PERSONAL_AWS_SECRET_ACCESS_KEY;
if (personalKeyId && personalSecretKey) {
  credentialsProvider = async () => {
    if (!personalKeyId || !personalSecretKey) throw new Error("PERSONAL AWS keys missing/empty.");
    if (personalKeyId.length < 16 || personalSecretKey.length < 30) console.warn("Warning: PERSONAL AWS keys appear short.");
    return { accessKeyId: personalKeyId, secretAccessKey: personalSecretKey };
  };
} else {
  credentialsProvider = defaultProvider();
}

let ddbClient;
try {
  ddbClient = new DynamoDBClient({ region: AWS_REGION, credentials: credentialsProvider });
} catch (clientInitError) {
  console.error(clientInitError);
  process.exit(1);
}

const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

function getNextMidnightTimestamp() {
  const now = new Date();
  const nextMidnight = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1));
  return Math.floor(nextMidnight.getTime() / 1000);
}

const LEADERBOARD_ATTRIBUTES = [
  'streak',
  'messages',
  'highestStreak',
  'messageLeaderWins',
  'averageMessagesPerDay',
  'level',
  'totalXp',
  'activeDaysCount',
  'longestInactivePeriod',
  'mostConsecutiveLeader'
];

const EXPIRING_ATTRIBUTES = [
  'streak',
  'activeDaysCount'
];

function safeParseNumber(value, defaultValue = 0) {
  if (value === null || value === undefined) return defaultValue;
  if (typeof value === 'number' && !isNaN(value)) return value;
  const parsed = Number(value);
  return isNaN(parsed) ? defaultValue : parsed;
}

function createAttributeItem(guildId, userId, attributeName, rawCount) {
  const count = safeParseNumber(rawCount, null);
  if (count === null || typeof count !== 'number') return null;
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
  if (!userId) return null;
  const primaryUserId = String(userId);
  const params = { TableName: TABLE_NAME, Key: { DiscordId: primaryUserId } };
  try {
    const result = await ddbDocClient.send(new GetCommand(params));
    return result.Item || null;
  } catch (error) {
    console.error(error);
    return null;
  }
}

async function getUserData(guildId, userId) {
  const rawItem = await getRawUserData(userId);
  if (!rawItem || typeof rawItem.userData !== 'object') return null;
  const ud = rawItem.userData;
  const wrapperKeys = ['S','N','BOOL','L','M','NULL'];
  const needsMigration = Object.values(ud).some(v => v && typeof v === 'object' && Object.keys(v).some(k => wrapperKeys.includes(k)));
  let userDataObj;
  if (needsMigration) {
    try {
      userDataObj = unmarshall(ud);
    } catch (err) {
      console.error(err);
      return null;
    }
  } else {
    userDataObj = ud;
  }
  return userDataObj;
}

async function saveUserData(guildId, userId, userData) {
  if (!userId || !guildId || !userData) throw new Error("saveUserData missing required arguments.");
  const now = new Date().toISOString();
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const primaryItem = { DiscordId: primaryUserId, guildId: primaryGuildId, userData: userData, lastUpdated: now };
  Object.keys(primaryItem).forEach(k => { if (primaryItem[k] === undefined) delete primaryItem[k]; });
  if (primaryItem.userData && typeof primaryItem.userData === 'object') {
    Object.keys(primaryItem.userData).forEach(k => { if (primaryItem.userData[k] === undefined) delete primaryItem.userData[k]; });
  } else {
    primaryItem.userData = {};
  }
  const itemsToPut = [primaryItem];
  for (const attrName of LEADERBOARD_ATTRIBUTES) {
    let countValue;
    if (attrName === 'level') countValue = userData.experience?.level;
    else if (attrName === 'totalXp') countValue = userData.experience?.totalXp;
    else countValue = userData[attrName];
    const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
    if (attributeItem) itemsToPut.push(attributeItem);
  }
  const putPromises = itemsToPut.map(item => {
    const params = { TableName: TABLE_NAME, Item: item };
    return ddbDocClient.send(new PutCommand(params))
        .then(() => ({ success: true, id: item.DiscordId }))
        .catch(error => ({ success: false, id: item.DiscordId, error: error }));
  });
  try {
    const results = await Promise.all(putPromises);
    const failedPuts = results.filter(r => !r.success);
    const primaryFailed = failedPuts.some(f => f.id === String(userId));
    if (primaryFailed) throw failedPuts.find(f => f.id === String(userId)).error || new Error(`Failed primary save for ${userId}.`);
  } catch (error) {
    throw error;
  }
}

async function updateUserData(guildId, userId, updates) {
  if (!userId || !guildId) throw new Error("updateUserData missing required arguments.");
  const updateKeys = Object.keys(updates);
  if (updateKeys.length === 0) return;
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
    const value = updates[key];
    if (value === undefined) continue;
    for (let i = 0; i < keyParts.length; i++) {
      const part = keyParts[i];
      const namePlaceholder = `#attr_${i}_${part.replace(/[^a-zA-Z0-9_]/g, '')}`;
      primaryExpressionAttributeNames[namePlaceholder] = part;
      pathExpression += `.${namePlaceholder}`;
      currentPathForValuePlaceholder += (i > 0 ? '_' : '') + part.replace(/[^a-zA-Z0-9_]/g, '');
    }
    const valuePlaceholder = `:${currentPathForValuePlaceholder}`;
    primaryExpressionAttributeValues[valuePlaceholder] = value;
    primaryUpdateParts.push(`${pathExpression} = ${valuePlaceholder}`);
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
    if (key.startsWith('experience.')) attrName = key.split('.')[1];
    if (LEADERBOARD_ATTRIBUTES.includes(attrName)) {
      const countValue = updates[key];
      const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
      if (attributeItem) {
        const params = { TableName: TABLE_NAME, Item: attributeItem };
        attributePutPromises.push(ddbDocClient.send(new PutCommand(params))
            .then(() => ({ success: true, id: attributeItem.DiscordId }))
            .catch(error => ({ success: false, id: attributeItem.DiscordId, error: error })));
      }
    }
  }
  let primaryUpdateError = null;
  try {
    await ddbDocClient.send(new UpdateCommand(primaryUpdateParams));
  } catch (error) {
    primaryUpdateError = error;
  }
  let attributePutResults = [];
  if (attributePutPromises.length) {
    attributePutResults = await Promise.all(attributePutPromises);
  }
  const failedAttributePuts = attributePutResults.filter(r => !r.success);
  if (primaryUpdateError) throw primaryUpdateError;
  if (updates.hasOwnProperty('messages') && failedAttributePuts.some(f => f.id === `${primaryUserId}-messages`)) {
    throw failedAttributePuts.find(f => f.id === `${primaryUserId}-messages`).error || new Error(`Messages attribute update failed for user ${primaryUserId}`);
  }
}

async function listUserData(guildId) {
  if (!guildId) throw new Error("listUserData missing guildId.");
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
  do {
    if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
    const data = await ddbDocClient.send(new ScanCommand(params));
    if (data.Items) allItems = allItems.concat(data.Items);
    lastEvaluatedKey = data.LastEvaluatedKey;
  } while (lastEvaluatedKey);
  return allItems
      .filter(item => item.DiscordId && item.userData && typeof item.userData === 'object')
      .map(item => ({ userId: item.DiscordId, userData: item.userData }));
}

async function incrementMessageLeaderWins(guildId, userId) {
  const currentData = await getUserData(guildId, userId);
  if (!currentData) {
    await updateUserData(guildId, userId, { messageLeaderWins: 1 });
  } else {
    const currentWins = safeParseNumber(currentData.messageLeaderWins, 0);
    await updateUserData(guildId, userId, { messageLeaderWins: currentWins + 1 });
  }
}

async function queryLeaderboard(attributeName, guildId, limit = 10) {
  if (!attributeName || !guildId || !LEADERBOARD_ATTRIBUTES.includes(attributeName)) return [];
  const primaryGuildId = String(guildId);
  const queryLimit = Math.max(1, Math.min(limit, 50));
  const params = {
    TableName: TABLE_NAME,
    IndexName: ATTRIBUTE_INDEX_NAME,
    KeyConditionExpression: '#attrName = :attrNameVal',
    FilterExpression: '#gid = :gidVal',
    ExpressionAttributeNames: { '#attrName': 'attributeName', '#gid': 'guildId' },
    ExpressionAttributeValues: { ':attrNameVal': attributeName, ':gidVal': primaryGuildId },
    ScanIndexForward: false,
    Limit: queryLimit
  };
  try {
    const result = await ddbDocClient.send(new QueryCommand(params));
    return result.Items || [];
  } catch {
    return [];
  }
}

async function batchResetMessageAttributes(guildId, userIds) {
  if (!guildId || !Array.isArray(userIds) || !userIds.length) return { success: false, processedCount: 0, successCount: 0, failCount: userIds?.length || 0 };
  const primaryGuildId = String(guildId);
  const attributeName = 'messages';
  const resetValue = 0;
  const BATCH_SIZE = 25;
  let writeRequests = [];
  for (const userId of userIds) {
    if (!userId) continue;
    const primaryUserId = String(userId);
    const item = createAttributeItem(primaryGuildId, primaryUserId, attributeName, resetValue);
    if (item) writeRequests.push({ PutRequest: { Item: item } });
  }
  let totalSucceeded = 0;
  let totalFailed = 0;
  for (let i = 0; i < writeRequests.length; i += BATCH_SIZE) {
    let batch = writeRequests.slice(i, i + BATCH_SIZE);
    let attempt = 0;
    while (batch.length && attempt < 5) {
      attempt++;
      const params = { RequestItems: { [TABLE_NAME]: batch } };
      const result = await ddbDocClient.send(new BatchWriteCommand(params));
      const unprocessed = result.UnprocessedItems?.[TABLE_NAME] || [];
      totalSucceeded += batch.length - unprocessed.length;
      batch = unprocessed;
    }
    totalFailed += batch.length;
  }
  return { success: totalFailed === 0, processedCount: writeRequests.length, successCount: totalSucceeded, failCount: totalFailed };
}

async function updatePrimaryUserMessages(guildId, userId, messageCount) {
  if (!guildId || !userId || typeof messageCount !== 'number') throw new Error("updatePrimaryUserMessages missing required arguments or invalid messageCount.");
  const primaryUserId = String(userId);
  const params = {
    TableName: TABLE_NAME,
    Key: { DiscordId: primaryUserId },
    UpdateExpression: "SET #ud.#msg = :msgVal, #lu = :luVal",
    ExpressionAttributeNames: { "#ud": "userData", "#msg": "messages", "#lu": "lastUpdated" },
    ExpressionAttributeValues: { ":msgVal": messageCount, ":luVal": new Date().toISOString() },
    ReturnValues: "NONE"
  };
  await ddbDocClient.send(new UpdateCommand(params));
}

module.exports = {
  getUserData,
  getRawUserData,
  saveUserData,
  updateUserData,
  listUserData,
  incrementMessageLeaderWins,
  queryLeaderboard,
  safeParseNumber,
  batchResetMessageAttributes,
  updatePrimaryUserMessages
};