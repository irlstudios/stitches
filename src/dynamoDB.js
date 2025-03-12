require('dotenv').config();
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  ScanCommand,
  UpdateCommand
} = require("@aws-sdk/lib-dynamodb");

const TABLE_NAME = process.env.DYNAMODB_TABLE || "DiscordAccounts";

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

function getNextMidnightTimestamp() {
  const now = new Date();
  const nextMidnight = new Date(now.getFullYear(), now.getMonth(), now.getDate() + 1);
  return Math.floor(nextMidnight.getTime() / 1000);
}

async function getUserData(guildId, userId) {
  try {
    const params = {
      TableName: TABLE_NAME,
      Key: { DiscordId: String(userId) }
    };
    const data = await ddbDocClient.send(new GetCommand(params));
    return data.Item || null;
  } catch (error) {
    console.error("Error getting user data:", error);
    return null;
  }
}

async function saveUserData(guildId, userId, userData) {
  userData.lastUpdated = new Date().toISOString();
  userData.expireAt = getNextMidnightTimestamp();

  try {
    const params = {
      TableName: TABLE_NAME,
      Item: {
        DiscordId: String(userId),
        ...userData
      }
    };
    await ddbDocClient.send(new PutCommand(params));
  } catch (error) {
    console.error("Error saving user data to DynamoDB:", error);
    throw error;
  }
}

async function listUserData(guildId) {
  const params = { TableName: TABLE_NAME };
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

    const final = [];
    for (const item of allItems) {
      if (item.DiscordId && item.hasOwnProperty('messages')) {
        final.push({ userId: item.DiscordId, userData: item });
      } else if (item.DiscordId && !item.attribute) {
        // main user record
        final.push({ userId: item.DiscordId, userData: item });
      }
    }
    return final;
  } catch (error) {
    console.error("Error listing user data from DynamoDB:", error);
    throw error;
  }
}

async function incrementMessageLeaderWins(userId) {
  try {
    const params = {
      TableName: TABLE_NAME,
      Key: { DiscordId: userId },
      UpdateExpression: "SET messageLeaderWins = if_not_exists(messageLeaderWins, :zero) + :inc",
      ExpressionAttributeValues: {
        ":inc": 1,
        ":zero": 0
      }
    };
    await ddbDocClient.send(new UpdateCommand(params));
  } catch (error) {
    console.error("Error incrementing message leader wins:", error);
    throw error;
  }
}

async function updateUserData(userId, updates) {
  updates.lastUpdated = new Date().toISOString();
  updates.expireAt = getNextMidnightTimestamp();

  const updateExpressions = [];
  const expressionAttributeNames = {};
  const expressionAttributeValues = {};

  for (const key of Object.keys(updates)) {
    updateExpressions.push(`#${key} = :${key}`);
    expressionAttributeNames[`#${key}`] = key;
    expressionAttributeValues[`:${key}`] = updates[key];
  }

  try {
    const params = {
      TableName: TABLE_NAME,
      Key: { DiscordId: userId },
      UpdateExpression: `SET ${updateExpressions.join(", ")}`,
      ExpressionAttributeNames: expressionAttributeNames,
      ExpressionAttributeValues: expressionAttributeValues
    };
    await ddbDocClient.send(new UpdateCommand(params));
  } catch (error) {
    console.error(`Error updating user data for ${userId}:`, error);
    throw error;
  }
}

module.exports = {
  getUserData,
  saveUserData,
  listUserData,
  incrementMessageLeaderWins,
  updateUserData
};