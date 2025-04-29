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
    console.error("[DynamoDB GetRaw] Missing userId.");
    return null;
  }
  const primaryUserId = String(userId);
  const params = { TableName: TABLE_NAME, Key: { DiscordId: primaryUserId } };
  console.log(`[DynamoDB GetRaw] User: ${primaryUserId} - Sending GetCommand.`);
  try {
    const result = await ddbDocClient.send(new GetCommand(params));
    console.log(`[DynamoDB GetRaw] User: ${primaryUserId} - GetCommand successful. Item received: ${!!result.Item}`);
    return result.Item || null;
  } catch (error) {
    console.error(`[DynamoDB GetRaw] User: ${primaryUserId} - Error during GetCommand:`, error);

    return null;
  }
}

async function getUserData(guildId, userId) {

  const rawItem = await getRawUserData(userId);
  if (rawItem && typeof rawItem.userData === 'object' && rawItem.userData !== null) {
    return rawItem.userData;
  } else if (rawItem && (!rawItem.userData || typeof rawItem.userData !== 'object')) {
    console.warn(`[DynamoDB GetUser] User: ${userId} - Found raw item but userData field is missing or not an object. Raw item keys:`, Object.keys(rawItem));
    return null;
  }

  return null;
}

async function saveUserData(guildId, userId, userData) {

  if (!userId || !guildId || !userData) {
    console.error("[DynamoDB Save] Missing required arguments.", { userId: !!userId, guildId: !!guildId, userData: !!userData });
    throw new Error("saveUserData missing required arguments.");
  }
  const now = new Date().toISOString();
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const primaryItem = { DiscordId: primaryUserId, guildId: primaryGuildId, userData: userData, lastUpdated: now };


  Object.keys(primaryItem).forEach(k => { if (primaryItem[k] === undefined) delete primaryItem[k]; });
  if (primaryItem.userData && typeof primaryItem.userData === 'object') {
    Object.keys(primaryItem.userData).forEach(k => { if (primaryItem.userData[k] === undefined) delete primaryItem.userData[k]; });
  } else { primaryItem.userData = {}; }

  const itemsToPut = [primaryItem];
  for (const attrName of LEADERBOARD_ATTRIBUTES) {
    let countValue;
    if (attrName === 'level') countValue = userData.experience?.level;
    else if (attrName === 'totalXp') countValue = userData.experience?.totalXp;
    else countValue = userData[attrName];
    const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
    if (attributeItem) itemsToPut.push(attributeItem);
  }

  console.log(`[DynamoDB Save] User: ${primaryUserId} - Preparing to put ${itemsToPut.length} items.`);
  const putPromises = itemsToPut.map(item => {
    const params = { TableName: TABLE_NAME, Item: item };

    return ddbDocClient.send(new PutCommand(params))
        .then(() => ({ success: true, id: item.DiscordId }))
        .catch(error => {
          console.error(`[DynamoDB Save] User: ${primaryUserId} - Error putting item ${item.DiscordId}:`, error);
          return { success: false, id: item.DiscordId, error: error };
        });
  });

  try {
    const results = await Promise.all(putPromises);
    const failedPuts = results.filter(r => !r.success);
    if (failedPuts.length > 0) {
      console.warn(`[DynamoDB Save] User: ${primaryUserId} - ${failedPuts.length}/${putPromises.length} puts failed. Failed IDs: [${failedPuts.map(f=>f.id).join(', ')}]`);
      const primaryFailed = failedPuts.some(f => f.id === primaryUserId);
      if (primaryFailed) {
        console.error(`[DynamoDB Save] User: ${primaryUserId} - CRITICAL: Failed primary item save.`);
        throw failedPuts.find(f => f.id === primaryUserId)?.error || new Error(`Failed primary save for ${primaryUserId}.`);
      }

    } else {
      console.log(`[DynamoDB Save] User: ${primaryUserId} - All ${putPromises.length} puts successful.`);
    }
  } catch (error) {
    console.error(`[DynamoDB Save] User: ${primaryUserId} - Error during saveUserData Promise.all or subsequent handling:`, error);
    throw error;
  }
}


async function updateUserData(guildId, userId, updates) {
  const functionStartTime = Date.now();
  if (!userId || !guildId) {
    console.error("[DynamoDB Update] Missing required arguments.", { userId: !!userId, guildId: !!guildId });
    throw new Error("updateUserData missing required arguments.");
  }
  const updateKeys = Object.keys(updates);
  if (updateKeys.length === 0) {
    console.warn(`[DynamoDB Update] User: ${userId} - Called with empty updates object. Skipping.`);
    return;
  }

  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  const now = new Date().toISOString();
  console.log(`[DynamoDB Update START] User: ${primaryUserId}, Guild: ${primaryGuildId}. Keys: [${updateKeys.join(', ')}]`);


  let primaryUpdateExpression = "SET ";
  const primaryExpressionAttributeNames = { '#ud': 'userData', '#lu': 'lastUpdated' };
  const primaryExpressionAttributeValues = { ':lu': now };
  const primaryUpdateParts = [];
  let validPrimaryUpdates = false;

  for (const key of updateKeys) {
    const keyParts = key.split('.');
    let pathExpression = '#ud';
    let currentPathForValuePlaceholder = '';
    let value = updates[key];

    if (value === undefined) {
      console.warn(`[DynamoDB Update] User: ${primaryUserId} - Skipping undefined value for key '${key}'.`);
      continue;
    }

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
    validPrimaryUpdates = true;
  }

  if (!validPrimaryUpdates) {
    console.warn(`[DynamoDB Update] User: ${primaryUserId} - No valid fields found for primary update. Only updating lastUpdated.`);

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


  const attributePutItems = [];
  const attributePutPromises = [];
  const attributeDetailsLog = [];
  for (const key of updateKeys) {
    let attrName = key;
    let countValue = updates[key];
    if (key.startsWith('experience.')) attrName = key.split('.')[1];

    if (LEADERBOARD_ATTRIBUTES.includes(attrName)) {
      const attributeItem = createAttributeItem(primaryGuildId, primaryUserId, attrName, countValue);
      if (attributeItem) {
        attributePutItems.push(attributeItem); // Keep track of items we intend to put
        attributeDetailsLog.push({ name: attrName, id: attributeItem.DiscordId, status: 'pending' });
        const params = { TableName: TABLE_NAME, Item: attributeItem };
        // Create promise immediately
        attributePutPromises.push(
            ddbDocClient.send(new PutCommand(params))
                .then(() => {
                  const detail = attributeDetailsLog.find(d => d.id === attributeItem.DiscordId);
                  if(detail) detail.status = 'success';
                  return { success: true, id: attributeItem.DiscordId };
                })
                .catch(error => {
                  const detail = attributeDetailsLog.find(d => d.id === attributeItem.DiscordId);
                  if(detail) detail.status = 'failed';
                  console.error(`[DynamoDB Update] User: ${primaryUserId} - Error during PutCommand for attribute ${attributeItem.DiscordId}:`, error);
                  return { success: false, id: attributeItem.DiscordId, error: error }; // Return failure object
                })
        );
      } else {

        attributeDetailsLog.push({ name: attrName, id: `${primaryUserId}-${attrName}`, status: 'skipped (invalid data)' });
      }
    }
  }


  let primaryUpdateError = null;
  try {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Sending primary UpdateCommand.`);
    await ddbDocClient.send(new UpdateCommand(primaryUpdateParams));
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Primary UpdateCommand successful.`);
  } catch (error) {
    console.error(`[DynamoDB Update] User: ${primaryUserId} - Error during primary UpdateCommand:`, error);
    primaryUpdateError = error;
  }


  let attributePutResults = [];
  if (attributePutPromises.length > 0) {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Waiting for ${attributePutPromises.length} attribute PutCommands to settle.`);
    attributePutResults = await Promise.all(attributePutPromises);
    console.log(`[DynamoDB Update] User: ${primaryUserId} - Finished attribute puts.`);
  } else {
    console.log(`[DynamoDB Update] User: ${primaryUserId} - No valid attribute items to put.`);
  }

  const successfulAttributePuts = attributePutResults.filter(r => r.success);
  const failedAttributePuts = attributePutResults.filter(r => !r.success);


  const functionEndTime = Date.now();
  console.log(`[DynamoDB Update END] User: ${primaryUserId}, Guild: ${primaryGuildId}. Duration: ${functionEndTime - functionStartTime}ms. Status:`);
  console.log(`  Primary Update Status: ${primaryUpdateError ? `FAILED (${primaryUpdateError.name})` : 'SUCCESS'}`);
  console.log(`  Attribute Updates: ${successfulAttributePuts.length} success, ${failedAttributePuts.length} failed, ${attributeDetailsLog.filter(d => d.status.startsWith('skipped')).length} skipped.`);
  attributeDetailsLog.forEach(d => console.log(`    - ${d.name} (ID: ${d.id}): ${d.status}`));


  if (primaryUpdateError) {
    console.error(`[DynamoDB Update] User: ${primaryUserId} - Throwing error because primary update failed.`);
    throw primaryUpdateError;
  }


  // **CRITICAL CHANGE for resetMessages:** If specifically updating 'messages', fail if the attribute put also failed.
  if (updates.hasOwnProperty('messages') && failedAttributePuts.some(f => f.id === `${primaryUserId}-messages`)) {
    console.error(`[DynamoDB Update] User: ${primaryUserId} - Throwing error because the 'messages' attribute update failed.`);
    throw failedAttributePuts.find(f => f.id === `${primaryUserId}-messages`)?.error || new Error(`Messages attribute update failed for user ${primaryUserId}`);
  }


  if (failedAttributePuts.length > 0) {
    console.warn(`[DynamoDB Update] User: ${primaryUserId} - Completed with ${failedAttributePuts.length} non-'messages' attribute update failure(s).`);

  }


  return;
}


async function listUserData(guildId) {
  if (!guildId) {
    console.error("[DynamoDB List] Missing guildId.");
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
  let page = 0;
  console.log(`[DynamoDB List] Guild: ${primaryGuildId} - Starting Scan.`);
  try {
    do {
      page++;
      if (lastEvaluatedKey) {
        params.ExclusiveStartKey = lastEvaluatedKey;
      }
      console.log(`[DynamoDB List] Guild: ${primaryGuildId} - Requesting page ${page}.`);
      const data = await ddbDocClient.send(new ScanCommand(params));
      if (data.Items) {
        allItems = allItems.concat(data.Items);
        console.log(`[DynamoDB List] Guild: ${primaryGuildId} - Page ${page} received ${data.Items.length} items. Total so far: ${allItems.length}.`);
      } else {
        console.log(`[DynamoDB List] Guild: ${primaryGuildId} - Page ${page} received 0 items.`);
      }
      lastEvaluatedKey = data.LastEvaluatedKey;
      if(lastEvaluatedKey) console.log(`[DynamoDB List] Guild: ${primaryGuildId} - More items exist, requesting next page.`);
    } while (lastEvaluatedKey);

    console.log(`[DynamoDB List] Guild: ${primaryGuildId} - Scan complete. Total items fetched: ${allItems.length}. Filtering...`);
    const filteredItems = allItems.filter(item => {
      if (!item.DiscordId) {
        console.warn(`[DynamoDB List] Guild: ${primaryGuildId} - Filtered out item with missing DiscordId. Item:`, JSON.stringify(item));
        return false;
      }
      if (!item.userData || typeof item.userData !== 'object') {
        console.warn(`[DynamoDB List] Guild: ${primaryGuildId} - Filtered out item for user ${item.DiscordId} with missing or invalid userData field. Keys:`, Object.keys(item));
        return false;
      }
      return true;
    });

    console.log(`[DynamoDB List] Guild: ${primaryGuildId} - Filtering complete. Returning ${filteredItems.length} valid user data records.`);
    return filteredItems.map(item => ({
      userId: item.DiscordId,
      userData: item.userData
    }));

  } catch (error) {
    console.error(`[DynamoDB List] Guild: ${primaryGuildId} - Error during Scan operation (Page ${page}):`, error);
    throw error;
  }
}

async function incrementMessageLeaderWins(guildId, userId) {
  if (!userId || !guildId) {
    console.error("[DynamoDB IncrementWins] Missing required arguments.", { userId: !!userId, guildId: !!guildId });
    throw new Error("incrementMessageLeaderWins missing required arguments.");
  }
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);
  console.log(`[DynamoDB IncrementWins] User: ${primaryUserId}, Guild: ${primaryGuildId} - Attempting to increment.`);
  try {
    const currentData = await getUserData(primaryGuildId, primaryUserId);
    if (!currentData) {
      console.warn(`[DynamoDB IncrementWins] User: ${primaryUserId}, Guild: ${primaryGuildId} - No existing data found. Initializing wins to 1.`);

      await updateUserData(primaryGuildId, primaryUserId, { messageLeaderWins: 1 });
    } else {
      const currentWins = safeParseNumber(currentData.messageLeaderWins, 0);
      console.log(`[DynamoDB IncrementWins] User: ${primaryUserId}, Guild: ${primaryGuildId} - Current wins: ${currentWins}. Incrementing to ${currentWins + 1}.`);

      await updateUserData(primaryGuildId, primaryUserId, { messageLeaderWins: currentWins + 1 });
    }
    console.log(`[DynamoDB IncrementWins] User: ${primaryUserId}, Guild: ${primaryGuildId} - Increment process completed.`);
  } catch (error) {

    console.error(`[DynamoDB IncrementWins] User: ${primaryUserId}, Guild: ${primaryGuildId} - Failed during increment process:`, error);
    throw error;
  }
}

async function queryLeaderboard(attributeName, guildId, limit = 10) {
  if (!attributeName || !guildId) {
    console.error("[DynamoDB QueryLB] Missing required arguments.", { attributeName: !!attributeName, guildId: !!guildId });
    return [];
  }
  if (!LEADERBOARD_ATTRIBUTES.includes(attributeName)) {
    console.error(`[DynamoDB QueryLB] Invalid attribute name: ${attributeName}`);
    return [];
  }
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
  console.log(`[DynamoDB QueryLB] Guild: ${primaryGuildId}, Attr: ${attributeName}, Limit: ${queryLimit} - Sending QueryCommand.`);
  try {
    const result = await ddbDocClient.send(new QueryCommand(params));
    console.log(`[DynamoDB QueryLB] Guild: ${primaryGuildId}, Attr: ${attributeName} - Query successful. Items received: ${result.Items?.length ?? 0}`);
    return result.Items || [];
  } catch (error) {
    console.error(`[DynamoDB QueryLB] Guild: ${primaryGuildId}, Attr: ${attributeName} - Error during QueryCommand:`, error);
    if (error.name === 'ResourceNotFoundException') console.error(`  Error Detail: GSI "${ATTRIBUTE_INDEX_NAME}" not found on table ${TABLE_NAME}.`);
    else if (error.name === 'ValidationException') console.error(`  Error Detail: Query Validation Error - ${error.message}.`);

    return [];
  }
}

async function batchResetMessageAttributes(guildId, userIds) {
  const functionStartTime = Date.now();
  if (!guildId || !Array.isArray(userIds) || userIds.length === 0) {
    console.error("[DynamoDB BatchReset] Missing guildId or invalid/empty userIds array.");

    return { success: false, processedCount: 0, successCount: 0, failCount: userIds?.length ?? 0, error: new Error("Missing guildId or invalid/empty userIds array.") };
  }

  const primaryGuildId = String(guildId);
  const attributeName = 'messages';
  const resetValue = 0;
  const now = new Date().toISOString();
  const BATCH_SIZE = 25;
  let writeRequests = [];
  let totalSucceeded = 0;
  let totalFailed = 0;


  for (const userId of userIds) {
    if (!userId) {
      console.warn(`[DynamoDB BatchReset] Guild: ${primaryGuildId} - Skipping invalid userId found in input list.`);
      continue;
    }
    const primaryUserId = String(userId);
    const item = createAttributeItem(primaryGuildId, primaryUserId, attributeName, resetValue);

    if (item) {
      writeRequests.push({ PutRequest: { Item: item } });
    } else {
      console.error(`[DynamoDB BatchReset] Guild: ${primaryGuildId} - Failed to create attribute item for user ${primaryUserId}.`);
      totalFailed++;
    }
  }
  const initialRequestCount = writeRequests.length;
  console.log(`[DynamoDB BatchReset START] Guild: ${primaryGuildId} - Prepared ${initialRequestCount} PutRequests for attribute '${attributeName}' reset.`);

  for (let i = 0; i < initialRequestCount; i += BATCH_SIZE) {
    const batch = writeRequests.slice(i, i + BATCH_SIZE);
    const batchNumber = Math.floor(i / BATCH_SIZE) + 1;
    const totalBatches = Math.ceil(initialRequestCount / BATCH_SIZE);
    console.log(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName} - Processing batch ${batchNumber}/${totalBatches} (Size: ${batch.length}).`);

    let attempt = 0;
    const MAX_ATTEMPTS = 5;
    let unprocessedItemsInBatch = batch;

    while (unprocessedItemsInBatch.length > 0 && attempt < MAX_ATTEMPTS) {
      attempt++;
      const currentAttemptSize = unprocessedItemsInBatch.length;
      const params = { RequestItems: { [TABLE_NAME]: unprocessedItemsInBatch } };
      console.log(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName}, Batch: ${batchNumber} - Attempt ${attempt}/${MAX_ATTEMPTS}: Sending BatchWriteCommand with ${currentAttemptSize} items.`);


      try {
        const result = await ddbDocClient.send(new BatchWriteCommand(params));
        const successfullyProcessedNow = currentAttemptSize - (result.UnprocessedItems?.[TABLE_NAME]?.length ?? 0);
        totalSucceeded += successfullyProcessedNow;

        if (result.UnprocessedItems && result.UnprocessedItems[TABLE_NAME] && result.UnprocessedItems[TABLE_NAME].length > 0) {
          unprocessedItemsInBatch = result.UnprocessedItems[TABLE_NAME];
          console.warn(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName}, Batch: ${batchNumber} - Attempt ${attempt}: Received ${unprocessedItemsInBatch.length} unprocessed items. Retrying.`);

          const delay = Math.random() * (200 * Math.pow(2, attempt));
          await new Promise(resolve => setTimeout(resolve, delay));
        } else {
          console.log(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName}, Batch: ${batchNumber} - Attempt ${attempt}: All ${currentAttemptSize} items in this batch attempt processed successfully.`);
          unprocessedItemsInBatch = [];
        }
      } catch (error) {
        console.error(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName}, Batch: ${batchNumber} - Attempt ${attempt}: Error during BatchWriteCommand:`, error);
        totalFailed += currentAttemptSize;
        unprocessedItemsInBatch = [];

        break;
      }
    }


    if (unprocessedItemsInBatch.length > 0) {
      const finalFailedCount = unprocessedItemsInBatch.length;
      totalFailed += finalFailedCount;
      const failedIds = unprocessedItemsInBatch.map(req => req.PutRequest?.Item?.DiscordId).filter(id => id);
      console.error(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName}, Batch: ${batchNumber} - CRITICAL: Failed to process ${finalFailedCount} items after ${MAX_ATTEMPTS} attempts. Failed Item DiscordIds: [${failedIds.join(', ')}]`);
    }
  }


  if (totalSucceeded + totalFailed !== initialRequestCount) {
    console.warn(`[DynamoDB BatchReset] Guild: ${primaryGuildId}, Attr: ${attributeName} - Count mismatch! Succeeded (${totalSucceeded}) + Failed (${totalFailed}) !== Initial Requests (${initialRequestCount}). Adjusting failure count.`);
    totalFailed = initialRequestCount - totalSucceeded;
  }

  const overallSuccess = totalFailed === 0;
  const functionEndTime = Date.now();
  console.log(`[DynamoDB BatchReset END] Guild: ${primaryGuildId}, Attr: ${attributeName}. Duration: ${functionEndTime - functionStartTime}ms. Total Submitted: ${initialRequestCount}, Succeeded: ${totalSucceeded}, Failed: ${totalFailed}.`);
  return { success: overallSuccess, processedCount: initialRequestCount, successCount: totalSucceeded, failCount: totalFailed };
}

async function updatePrimaryUserMessages(guildId, userId, messageCount) {
  const functionStartTime = Date.now();
  if (!userId || !guildId || typeof messageCount !== 'number') {
    console.error("[DynamoDB PrimaryMsgUpdate] Missing required arguments or invalid messageCount type.", { userId: !!userId, guildId: !!guildId, messageCountType: typeof messageCount });

    throw new Error("updatePrimaryUserMessages missing required arguments or invalid messageCount.");
  }
  const primaryUserId = String(userId);
  const primaryGuildId = String(guildId);


  const params = {
    TableName: TABLE_NAME,
    Key: { DiscordId: primaryUserId },
    UpdateExpression: "SET #ud.#msg = :msgVal, #lu = :luVal",
    ExpressionAttributeNames: { "#ud": "userData", "#msg": "messages", "#lu": "lastUpdated" },
    ExpressionAttributeValues: { ":msgVal": messageCount, ":luVal": new Date().toISOString() },
    ReturnValues: "NONE"
  };

  try {

    await ddbDocClient.send(new UpdateCommand(params));
    const functionEndTime = Date.now();
    console.log(`[DynamoDB PrimaryMsgUpdate] User: ${primaryUserId}, Guild: ${primaryGuildId}. Success. Duration: ${functionEndTime - functionStartTime}ms.`);
    return { success: true, userId: primaryUserId };
  } catch (error) {
    const functionEndTime = Date.now();
    console.error(`[DynamoDB PrimaryMsgUpdate] User: ${primaryUserId}, Guild: ${primaryGuildId}. Error updating primary messages. Duration: ${functionEndTime - functionStartTime}ms:`, error);

    return { success: false, userId: primaryUserId, error: error };
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