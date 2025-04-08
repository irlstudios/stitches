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
//const personalKeyId = process.env.PERSONAL_AWS_ACCESS_KEY_ID;
//const personalSecretKey = process.env.PERSONAL_AWS_SECRET_ACCESS_KEY;

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

function createAttributeItem(guildId, userId, attributeName, count) {
  if (typeof count !== 'number' || isNaN(count)) {
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
  try {
    const params = { TableName: TABLE_NAME, Key: { DiscordId: String(userId) } };
    const { Item } = await ddbDocClient.send(new GetCommand(params));
    return Item || null;
  } catch (error) {
    console.error(`[DynamoDB] Error in getRawUserData for userId ${userId}:`, error);
    return null;
  }
}

async function getUserData(guildId, userId) {
  const rawItem = await getRawUserData(userId);
  if (rawItem && typeof rawItem.userData === 'object' && rawItem.userData !== null) {
    return rawItem.userData;
  }
  return null;
}

async function saveUserData(guildId, userId, userData) {
  if (!userId || !guildId || !userData) { console.error("[DynamoDB] saveUserData missing args."); return; }
  const now = new Date().toISOString();
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);

  const primaryItem = {
    DiscordId: primaryUserId, guildId: primaryGuildId,
    userData: userData, lastUpdated: now
  };
  Object.keys(primaryItem).forEach(k => primaryItem[k] === undefined && delete primaryItem[k]);
  if (primaryItem.userData) {
    Object.keys(primaryItem.userData).forEach(k => {
      if (primaryItem.userData[k] === undefined) delete primaryItem.userData[k];
      if(primaryItem.userData.experience && typeof primaryItem.userData.experience === 'object') Object.keys(primaryItem.userData.experience).forEach(ek => primaryItem.userData.experience[ek] === undefined && delete primaryItem.userData.experience[ek]);
      if(primaryItem.userData.lastMessage && typeof primaryItem.userData.lastMessage === 'object') Object.keys(primaryItem.userData.lastMessage).forEach(lk => primaryItem.userData.lastMessage[lk] === undefined && delete primaryItem.userData.lastMessage[lk]);
      if(primaryItem.userData.mentionsRepliesCount && typeof primaryItem.userData.mentionsRepliesCount === 'object') Object.keys(primaryItem.userData.mentionsRepliesCount).forEach(mk => primaryItem.userData.mentionsRepliesCount[mk] === undefined && delete primaryItem.userData.mentionsRepliesCount[mk]);
    });
  }

  const itemsToPut = [primaryItem];
  for (const attrName of LEADERBOARD_ATTRIBUTES) {
    let count;
    if (attrName === 'level') count = userData.experience?.level;
    else if (attrName === 'totalXp') count = userData.experience?.totalXp;
    else count = userData[attrName];
    const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, count);
    if (attributeItem) itemsToPut.push(attributeItem);
  }

  const putPromises = itemsToPut.map(item => {
    const params = { TableName: TABLE_NAME, Item: item };
    return ddbDocClient.send(new PutCommand(params)).catch(error => {
      console.error(`[DynamoDB] Error putting item ${item.DiscordId} during save:`, error);
      return null;
    });
  });

  try {
    const results = await Promise.all(putPromises);
    const failedPuts = results.filter(r => r === null).length;
    if (failedPuts > 0) console.warn(`[DynamoDB] ${failedPuts}/${putPromises.length} puts failed during save for ${userId}.`);
    if (results[0] === null) throw new Error(`Failed primary save for ${userId}.`);
  } catch (error) {
    console.error(`[DynamoDB] Major error during saveUserData for ${userId}:`, error);
    throw error;
  }
}

async function updateUserData(guildId, userId, updates) {
  if (!userId || !guildId) { console.error("[DynamoDB] updateUserData missing args."); return; }
  const updateKeys = Object.keys(updates);
  if (updateKeys.length === 0) { return; }

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
    for (let i = 0; i < keyParts.length; i++) {
      const part = keyParts[i];
      const namePlaceholder = `#attr_${i}_${part.replace(/[^a-zA-Z0-9_]/g, '')}`;
      primaryExpressionAttributeNames[namePlaceholder] = part;
      pathExpression += `.${namePlaceholder}`;
      currentPathForValuePlaceholder += (i > 0 ? '_' : '') + part.replace(/[^a-zA-Z0-9_]/g, '');
    }
    const valuePlaceholder = `:${currentPathForValuePlaceholder}`;
    primaryExpressionAttributeValues[valuePlaceholder] = updates[key];
    primaryUpdateParts.push(`${pathExpression} = ${valuePlaceholder}`);
  }
  primaryUpdateParts.push(`#lu = :lu`);
  primaryUpdateExpression += primaryUpdateParts.join(", ");
  const primaryUpdateParams = {
    TableName: TABLE_NAME, Key: { DiscordId: primaryUserId },
    UpdateExpression: primaryUpdateExpression,
    ExpressionAttributeNames: primaryExpressionAttributeNames,
    ExpressionAttributeValues: primaryExpressionAttributeValues,
    ReturnValues: "NONE"
  };

  const attributePutPromises = [];
  for (const key of updateKeys) {
    let attrName = key, count = updates[key];
    if (key.startsWith('experience.')) attrName = key.split('.')[1];
    if (LEADERBOARD_ATTRIBUTES.includes(attrName)) {
      const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, count);
      if (attributeItem) {
        attributePutPromises.push(
            ddbDocClient.send(new PutCommand({ TableName: TABLE_NAME, Item: attributeItem })).catch(error => {
              console.error(`[DynamoDB] Error putting attribute ${attrName} during update for ${userId}:`, error);
              return null;
            })
        );
      }
    }
  }

  try {
    await ddbDocClient.send(new UpdateCommand(primaryUpdateParams));
    if (attributePutPromises.length > 0) {
      const results = await Promise.all(attributePutPromises);
      const failedPuts = results.filter(r => r === null).length;
      if (failedPuts > 0) console.warn(`[DynamoDB] ${failedPuts}/${attributePutPromises.length} attribute puts failed during update for ${userId}.`);
    }
  } catch (error) {
    console.error(`[DynamoDB] Error updating user data for userId ${userId}:`, error);
    if (error.name === 'ValidationException') { console.error("[DynamoDB Validation Debug] Expression:", primaryUpdateParams.UpdateExpression); }
    throw error;
  }
}

async function listUserData(guildId) {
  if (!guildId) { console.error("[DynamoDB] listUserData missing guildId."); return []; }
  const params = {
    TableName: TABLE_NAME,
    FilterExpression: "#gid = :gid AND attribute_not_exists(attributeName)",
    ExpressionAttributeNames: { "#gid": "guildId" },
    ExpressionAttributeValues: { ":gid": String(guildId) },
    ProjectionExpression: "DiscordId, userData"
  };
  let allItems = [], lastEvaluatedKey = null;
  try {
    do {
      if (lastEvaluatedKey) params.ExclusiveStartKey = lastEvaluatedKey;
      const data = await ddbDocClient.send(new ScanCommand(params));
      if (data.Items) allItems = allItems.concat(data.Items);
      lastEvaluatedKey = data.LastEvaluatedKey;
    } while (lastEvaluatedKey);
    return allItems.filter(i => i.DiscordId && i.userData).map(i => ({ userId: i.DiscordId, userData: i.userData }));
  } catch (error) {
    console.error(`[DynamoDB] Error listing primary user data for guildId ${guildId}:`, error);
    throw error;
  }
}

async function incrementMessageLeaderWins(guildId, userId) {
  if (!userId || !guildId) { console.error("[DynamoDB] incrementMessageLeaderWins missing args."); return; }
  try {
    const currentData = await getUserData(guildId, userId);
    const currentWins = currentData?.messageLeaderWins ?? 0;
    await updateUserData(guildId, userId, { messageLeaderWins: currentWins + 1 });
  } catch (error) {
    console.error(`[DynamoDB] Failed during incrementMessageLeaderWins for userId ${userId}:`, error);
    throw error;
  }
}

async function queryLeaderboard(attributeName, guildId, limit = 10) {
  if (!attributeName || !guildId) { console.error("[DynamoDB] queryLeaderboard missing args."); return []; }
  if (!LEADERBOARD_ATTRIBUTES.includes(attributeName)) { console.error(`[DynamoDB] queryLeaderboard invalid attr: ${attributeName}`); return []; }
  try {
    const params = {
      TableName: TABLE_NAME, IndexName: ATTRIBUTE_INDEX_NAME,
      KeyConditionExpression: '#attrName = :attrNameVal',
      FilterExpression: '#gid = :gidVal',
      ExpressionAttributeNames: { '#attrName': 'attributeName', '#gid': 'guildId' },
      ExpressionAttributeValues: { ':attrNameVal': attributeName, ':gidVal': String(guildId) },
      ScanIndexForward: false,
      Limit: Math.max(1, Math.min(limit, 100))
    };
    const { Items } = await ddbDocClient.send(new QueryCommand(params));
    return Items || [];
  } catch (error) {
    console.error(`[DynamoDB] Error querying leaderboard for attr "${attributeName}" guild ${guildId}:`, error);
    if (error.name === 'ResourceNotFoundException') console.error(`[DynamoDB] GSI "${ATTRIBUTE_INDEX_NAME}" not found.`);
    else if (error.name === 'ValidationException' && (error.message.includes('NUMBER'))) console.error(`[DynamoDB] GSI "${ATTRIBUTE_INDEX_NAME}" sort key 'count' MUST be Number type!`);
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
  queryLeaderboard
};