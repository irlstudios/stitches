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
} catch (e) {
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
  const count = safeParseNumber(rawCount, 0);
  if (typeof count !== 'number') return null;
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
  } catch {
    return null;
  }
}

async function getUserData(guildId, userId) {
  const rawItem = await getRawUserData(userId);
  if (!rawItem) return null;
  if (typeof rawItem.userData === 'object' && rawItem.userData !== null) {
    return rawItem.userData;
  }
  if (rawItem.attributeName) return null;
  try {
    const unmarshalled = unmarshall(rawItem);
    const userDataObj = unmarshalled.userData || unmarshalled;
    await saveUserData(guildId, userId, userDataObj);
    return userDataObj;
  } catch {
    return null;
  }
}

async function saveUserData(guildId, userId, userData) {
  if (!userId || !guildId || !userData) throw new Error("saveUserData missing required arguments.");
  console.log(`[DynamoDB Save] userData keys: ${Object.keys(userData).join(',')}`);
  console.log(`[DynamoDB Save] experience object: ${JSON.stringify(userData.experience)}`);
  const now = new Date().toISOString();
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const primaryItem = { DiscordId: primaryUserId, guildId: primaryGuildId, userData: userData, lastUpdated: now };
  Object.keys(primaryItem).forEach(k => { if (primaryItem[k] === undefined) delete primaryItem[k]; });
  const itemsToPut = [primaryItem];
  for (const attrName of LEADERBOARD_ATTRIBUTES) {
    let countValue;
    if (attrName === 'level') countValue = userData.experience?.level;
    else if (attrName === 'totalXp') countValue = userData.experience?.totalXp;
    else countValue = userData[attrName];
    console.log(`[DynamoDB Save] processing attribute ${attrName} with value ${countValue}`);
    const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
    if (attributeItem) itemsToPut.push(attributeItem);
  }
  const putPromises = itemsToPut.map(item => {
    const params = { TableName: TABLE_NAME, Item: item };
    return ddbDocClient.send(new PutCommand(params))
        .then(() => ({ success: true, id: item.DiscordId }))
        .catch(error => ({ success: false, id: item.DiscordId, error: error }));
  });
  const results = await Promise.all(putPromises);
  const failedPuts = results.filter(r => !r.success);
  if (failedPuts.some(f => f.id === String(userId))) {
    throw failedPuts.find(f => f.id === String(userId)).error || new Error(`Failed primary save for ${userId}.`);
  }
}

async function updateUserData(guildId, userId, updates) {
  if (!userId || !guildId) throw new Error("updateUserData missing required arguments.");
  const updateKeys = Object.keys(updates);
  if (!updateKeys.length) return;
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const now = new Date().toISOString();
  let updateExpr = "SET ";
  const exprNames = { '#ud': 'userData', '#lu': 'lastUpdated' };
  const exprValues = { ':lu': now };
  const parts = [];
  for (const key of updateKeys) {
    const keyParts = key.split('.');
    let path = '#ud';
    let placeholder = '';
    const value = updates[key];
    if (value === undefined) continue;
    for (let i = 0; i < keyParts.length; i++) {
      const part = keyParts[i];
      const namePh = `#attr_${i}_${part.replace(/[^a-zA-Z0-9_]/g, '')}`;
      exprNames[namePh] = part;
      path += `.${namePh}`;
      placeholder += (i ? '_' : '') + part.replace(/[^a-zA-Z0-9_]/g, '');
    }
    const valuePh = `:${placeholder}`;
    exprValues[valuePh] = value;
    parts.push(`${path} = ${valuePh}`);
  }
  parts.push(`#lu = :lu`);
  updateExpr += parts.join(", ");
  const params = {
    TableName: TABLE_NAME,
    Key: { DiscordId: primaryUserId },
    UpdateExpression: updateExpr,
    ExpressionAttributeNames: exprNames,
    ExpressionAttributeValues: exprValues,
    ReturnValues: "NONE"
  };
  const attributePromises = [];
  for (const key of updateKeys) {
    let attrName = key.startsWith('experience.') ? key.split('.')[1] : key;
    if (LEADERBOARD_ATTRIBUTES.includes(attrName)) {
      const countValue = updates[key];
      const item = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
      if (item) attributePromises.push(ddbDocClient.send(new PutCommand({ TableName: TABLE_NAME, Item: item }))
          .then(() => ({ success: true, id: item.DiscordId }))
          .catch(error => ({ success: false, id: item.DiscordId, error: error })));
    }
  }
  let primaryError = null;
  try {
    await ddbDocClient.send(new UpdateCommand(params));
  } catch (e) {
    primaryError = e;
  }
  const results = attributePromises.length ? await Promise.all(attributePromises) : [];
  if (primaryError) throw primaryError;
  if (updates.hasOwnProperty('messages') && results.some(r => !r.success && r.id === `${primaryUserId}-messages`)) {
    throw results.find(r => r.id === `${primaryUserId}-messages`).error || new Error(`Messages attribute update failed for user ${primaryUserId}`);
  }
}

async function listUserData(guildId) {
  if (!guildId) throw new Error("listUserData missing guildId.");
  const gid = String(guildId);
  const params = {
    TableName: TABLE_NAME,
    FilterExpression: "#gid = :gid AND attribute_not_exists(attributeName)",
    ExpressionAttributeNames: { "#gid": "guildId" },
    ExpressionAttributeValues: { ":gid": gid },
    ProjectionExpression: "DiscordId, userData"
  };
  let items = [];
  let lastKey = null;
  do {
    if (lastKey) params.ExclusiveStartKey = lastKey;
    const data = await ddbDocClient.send(new ScanCommand(params));
    if (data.Items) items = items.concat(data.Items);
    lastKey = data.LastEvaluatedKey;
  } while (lastKey);
  return items.filter(i => i.DiscordId && i.userData && typeof i.userData === 'object')
      .map(i => ({ userId: i.DiscordId, userData: i.userData }));
}

async function incrementMessageLeaderWins(guildId, userId) {
  const data = await getUserData(guildId, userId);
  if (!data) {
    await updateUserData(guildId, userId, { messageLeaderWins: 1 });
  } else {
    const wins = safeParseNumber(data.messageLeaderWins, 0);
    await updateUserData(guildId, userId, { messageLeaderWins: wins + 1 });
  }
}

async function queryLeaderboard(attributeName, guildId, limit = 10) {
  if (!attributeName || !guildId || !LEADERBOARD_ATTRIBUTES.includes(attributeName)) return [];
  const gid = String(guildId);
  const qLimit = Math.max(1, Math.min(limit, 50));
  const params = {
    TableName: TABLE_NAME,
    IndexName: ATTRIBUTE_INDEX_NAME,
    KeyConditionExpression: '#attr = :attrVal',
    FilterExpression: '#gid = :gidVal',
    ExpressionAttributeNames: { '#attr': 'attributeName', '#gid': 'guildId' },
    ExpressionAttributeValues: { ':attrVal': attributeName, ':gidVal': gid },
    ScanIndexForward: false,
    Limit: qLimit
  };
  try {
    const res = await ddbDocClient.send(new QueryCommand(params));
    return res.Items || [];
  } catch {
    return [];
  }
}

async function batchResetMessageAttributes(guildId, userIds) {
  if (!guildId || !Array.isArray(userIds) || !userIds.length) return { success: false, processedCount: 0, successCount: 0, failCount: userIds?.length || 0 };
  const gid = String(guildId);
  const attr = 'messages';
  const BATCH_SIZE = 25;
  let requests = [];
  for (const u of userIds) {
    if (!u) continue;
    const primaryUser = String(u);
    const item = createAttributeItem(gid, primaryUser, attr, 0);
    if (item) requests.push({ PutRequest: { Item: item } });
  }
  let succeeded = 0;
  let failed = 0;
  for (let i = 0; i < requests.length; i += BATCH_SIZE) {
    let batch = requests.slice(i, i + BATCH_SIZE);
    let attempt = 0;
    while (batch.length && attempt < 5) {
      attempt++;
      const result = await ddbDocClient.send(new BatchWriteCommand({ RequestItems: { [TABLE_NAME]: batch } }));
      const unprocessed = result.UnprocessedItems?.[TABLE_NAME] || [];
      succeeded += batch.length - unprocessed.length;
      batch = unprocessed;
    }
    failed += batch.length;
  }
  return { success: failed === 0, processedCount: requests.length, successCount: succeeded, failCount: failed };
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