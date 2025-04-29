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
    console.warn(`[DynamoDB Update] updateUserData called for ${userId} with empty updates object. Skipping.`);
    return;
  }

  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const now = new Date().toISOString();
  console.log(`[DynamoDB Update] Starting update for User: ${primaryUserId}, Guild: ${primaryGuildId}. Keys: [${updateKeys.join(', ')}]`);

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
      console.warn(`[DynamoDB UpdateSanitize] User: ${primaryUserId} - Attempted to set undefined value for key '${key}'. Skipping this field.`);
      continue;
    }

    const valuePlaceholder = `:${currentPathForValuePlaceholder}`;
    primaryExpressionAttributeValues[valuePlaceholder] = value;
    primaryUpdateParts.push(`${pathExpression} = ${valuePlaceholder}`);
  }

  if (primaryUpdateParts.length === 0) {
    console.warn(`[DynamoDB UpdateSanitize] User: ${primaryUserId} - No valid update parts remained after sanitizing. Only updating lastUpdated.`);
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
  const attributeDetails = [];
  for (const key of updateKeys) {
    let attrName = key;
    let countValue = updates[key];

    if (key.startsWith('experience.')) {
      attrName = key.split('.')[1];
    }

    if (LEADERBOARD_ATTRIBUTES.includes(attrName)) {
      const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
      if (attributeItem) {
        attributeDetails.push({ id: attributeItem.DiscordId, name: attrName });
        attributePutPromises.push(
            ddbDocClient.send(new PutCommand({ TableName: TABLE_NAME, Item: attributeItem }))
                .then(() => ({ success: true, id: attributeItem.DiscordId, name: attrName }))
                .catch(error => {
                  console.error(`[DynamoDB Update] Error putting attribute item ${attributeItem.DiscordId} during updateUserData for ${primaryUserId}:`, error);
                  return { success: false, id: attributeItem.DiscordId, name: attrName, error: error };
                })
        );
      } else {
        console.log(`[DynamoDB Update] User: ${primaryUserId} - Skipped creating attribute item for '${attrName}' (value: ${countValue})`);
        attributeDetails.push({ id: `${primaryUserId}-${attrName}`, name: attrName, skipped: true });
      }
    }
  }

  let primaryUpdateSuccess = false;
  let primaryUpdateError = null;
  try {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Attempting primary UpdateItem.`);
    await ddbDocClient.send(new UpdateCommand(primaryUpdateParams));
    primaryUpdateSuccess = true;
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Primary UpdateItem successful.`);
  } catch (error) {
    primaryUpdateError = error;
    console.error(`[DynamoDB Update] User: ${primaryUserId} - Error during primary UpdateItem:`, error);
    if (error.name === 'ValidationException') {
      console.error("[DynamoDB Validation Debug] Expression:", primaryUpdateParams.UpdateExpression);
      console.error("[DynamoDB Validation Debug] Names:", JSON.stringify(primaryUpdateParams.ExpressionAttributeNames));
      console.error("[DynamoDB Validation Debug] Values:", JSON.stringify(primaryUpdateParams.ExpressionAttributeValues));
    }

  }

  let attributePutResults = [];
  if (attributePutPromises.length > 0) {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Waiting for ${attributePutPromises.length} attribute PutItem operations.`);
    attributePutResults = await Promise.all(attributePutPromises);
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Finished attribute PutItem operations.`);
  } else {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - No attribute PutItem operations were needed/generated.`);
  }

  const failedAttributePuts = attributePutResults.filter(r => r && r.success === false);
  const successfulAttributePuts = attributePutResults.filter(r => r && r.success === true);
  const skippedAttributePuts = attributeDetails.filter(d => d.skipped === true);

  if (failedAttributePuts.length > 0) {
    console.warn(`[DynamoDB Update] User: ${primaryUserId} - ${failedAttributePuts.length}/${attributeDetails.length} attribute puts failed. Failed attributes: [${failedAttributePuts.map(f=>f.name).join(', ')}]`);
  }
  if (successfulAttributePuts.length > 0) {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - ${successfulAttributePuts.length}/${attributeDetails.length} attribute puts successful. Successful attributes: [${successfulAttributePuts.map(f=>f.name).join(', ')}]`);
  }
  if (skippedAttributePuts.length > 0) {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - ${skippedAttributePuts.length}/${attributeDetails.length} attribute puts skipped (invalid data). Skipped attributes: [${skippedAttributePuts.map(f=>f.name).join(', ')}]`);
  }


  if (!primaryUpdateSuccess) {
    console.error(`[DynamoDB Update] User: ${primaryUserId} - Update failed due to primary UpdateItem error. Throwing error.`);

    throw primaryUpdateError || new Error("Primary UpdateItem failed for unknown reason.");
  }

  if (failedAttributePuts.length > 0) {
    console.warn(`[DynamoDB Update] User: ${primaryUserId} - Update completed, but ${failedAttributePuts.length} attribute updates failed. Check logs for details.`);

  }

  console.log(`[DynamoDB Update] Finished update for User: ${primaryUserId}, Guild: ${primaryGuildId}. Primary Success: ${primaryUpdateSuccess}. Attribute Success: ${successfulAttributePuts.length}/${attributeDetails.length}.`);

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

async function batchResetMessageAttributes(guildId, userIds) {
  if (!guildId || !Array.isArray(userIds) || userIds.length === 0) {
    console.error("[DynamoDB Batch] batchResetMessageAttributes missing guildId or invalid/empty userIds array.");
    return { success: false, processedCount: 0, successCount: 0, failCount: 0, unprocessedItems: userIds.length };
  }

  const primaryGuildId = String(guildId);
  const attributeName = 'messages';
  const resetValue = 0;
  const now = new Date().toISOString();
  const BATCH_SIZE = 25;
  let writeRequests = [];
  let totalProcessed = 0;
  let totalSuccess = 0;
  let totalFailed = 0;
  let totalUnprocessedInitially = 0;

  for (const userId of userIds) {
    const primaryUserId = String(userId);
    const item = {
      DiscordId: `${primaryUserId}-${attributeName}`,
      attributeName: attributeName,
      count: resetValue,
      discordAccountId: primaryUserId,
      guildId: primaryGuildId,
      lastUpdated: now
    };
    if (EXPIRING_ATTRIBUTES.includes(attributeName)) {
      item.expireAt = getNextMidnightTimestamp();
    }
    writeRequests.push({ PutRequest: { Item: item } });
  }

  console.log(`[DynamoDB Batch] Prepared ${writeRequests.length} PutRequests for attribute '${attributeName}' reset in guild ${primaryGuildId}.`);

  for (let i = 0; i < writeRequests.length; i += BATCH_SIZE) {
    const batch = writeRequests.slice(i, i + BATCH_SIZE);
    const params = { RequestItems: { [TABLE_NAME]: batch } };
    let attempt = 0;
    const MAX_ATTEMPTS = 5;
    let unprocessedItems = batch; // Start with the full batch for the first attempt

    while (unprocessedItems.length > 0 && attempt < MAX_ATTEMPTS) {
      attempt++;
      const currentBatchSize = unprocessedItems.length;
      totalProcessed += currentBatchSize; // Count attempts on items
      params.RequestItems[TABLE_NAME] = unprocessedItems;
      console.log(`[DynamoDB Batch] Guild ${primaryGuildId} - Attempt ${attempt}/${MAX_ATTEMPTS}: Sending BatchWriteCommand with ${currentBatchSize} items (Attribute: ${attributeName}).`);

      try {
        const result = await ddbDocClient.send(new BatchWriteCommand(params));
        if (result.UnprocessedItems && result.UnprocessedItems[TABLE_NAME] && result.UnprocessedItems[TABLE_NAME].length > 0) {
          unprocessedItems = result.UnprocessedItems[TABLE_NAME];
          totalUnprocessedInitially += unprocessedItems.length; // Count items returned as unprocessed
          console.warn(`[DynamoDB Batch] Guild ${primaryGuildId} - Attempt ${attempt}: Received ${unprocessedItems.length} unprocessed items for attribute '${attributeName}'. Retrying.`);
          await new Promise(resolve => setTimeout(resolve, 200 * Math.pow(2, attempt))); // Exponential backoff
        } else {
          console.log(`[DynamoDB Batch] Guild ${primaryGuildId} - Attempt ${attempt}: Batch processed successfully (Attribute: ${attributeName}).`);
          totalSuccess += currentBatchSize; // Mark success for the items in this attempt
          unprocessedItems = []; // Exit loop
        }
      } catch (error) {
        console.error(`[DynamoDB Batch] Guild ${primaryGuildId} - Attempt ${attempt}: Error during BatchWriteCommand for attribute '${attributeName}':`, error);

        totalFailed += currentBatchSize; // Assume all items in this failed attempt are failures
        unprocessedItems = []; // Exit loop on hard error
        // Re-throw might be desired depending on how critical perfect completion is
        // throw error;
      }
    }

    // If items remain unprocessed after max attempts
    if (unprocessedItems.length > 0) {
      console.error(`[DynamoDB Batch] Guild ${primaryGuildId} - CRITICAL: Failed to process ${unprocessedItems.length} items for attribute '${attributeName}' after ${MAX_ATTEMPTS} attempts. Logging unprocessed items.`);
      // unprocessedItems contains the raw request objects, extract IDs if needed for logging
      const failedIds = unprocessedItems.map(req => req.PutRequest?.Item?.DiscordId).filter(id => id);
      console.error(`[DynamoDB Batch] Guild ${primaryGuildId} - Failed Item DiscordIds (Attribute: ${attributeName}): [${failedIds.join(', ')}]`);
      totalFailed += unprocessedItems.length; // Add finally unprocessed to failed count
    }
  }

  // Adjust success count based on final failures
  totalSuccess = userIds.length - totalFailed;


  console.log(`[DynamoDB Batch] Finished batch reset for attribute '${attributeName}' in guild ${primaryGuildId}. Total Users: ${userIds.length}, Succeeded: ${totalSuccess}, Failed: ${totalFailed}.`);
  return { success: totalFailed === 0, processedCount: userIds.length, successCount: totalSuccess, failCount: totalFailed, unprocessedItems: totalFailed };
}

async function updatePrimaryUserMessages(guildId, userId, messageCount) {
  if (!userId || !guildId || typeof messageCount !== 'number') {
    console.error("[DynamoDB PrimaryUpdate] updatePrimaryUserMessages missing required arguments or invalid messageCount type.", { userId: !!userId, guildId: !!guildId, messageCountType: typeof messageCount });
    throw new Error("updatePrimaryUserMessages missing required arguments or invalid messageCount.");
  }
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const now = new Date().toISOString();
  console.log(`[DynamoDB PrimaryUpdate] Starting update for User: ${primaryUserId}, Guild: ${primaryGuildId}. Setting messages to ${messageCount}.`);

  const params = {
    TableName: TABLE_NAME,
    Key: { DiscordId: primaryUserId },
    UpdateExpression: "SET #ud.#msg = :msgVal, #lu = :luVal",
    ExpressionAttributeNames: {
      "#ud": "userData",
      "#msg": "messages",
      "#lu": "lastUpdated"
    },
    ExpressionAttributeValues: {
      ":msgVal": messageCount,
      ":luVal": now
    },
    ReturnValues: "NONE"
  };

  try {
    await ddbDocClient.send(new UpdateCommand(params));
    console.log(`[DynamoDB PrimaryUpdate] Successfully updated primary messages for User: ${primaryUserId}, Guild: ${primaryGuildId}.`);
    return { success: true };
  } catch (error) {
    console.error(`[DynamoDB PrimaryUpdate] Error updating primary messages for User: ${primaryUserId}, Guild: ${primaryGuildId}:`, error);
    if (error.name === 'ValidationException') {
      console.error("[DynamoDB Validation Debug] PrimaryUpdate Params:", JSON.stringify(params));
    }
    // Do not throw here, allow Promise.all in caller to handle
    return { success: false, error: error };
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
  safeParseNumber,
  batchResetMessageAttributes,
  updatePrimaryUserMessages
};