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

/**
 * @param {string} userId
 * @returns {Promise<Object|null>}
 */

async function getUserData(userId) {
  const params = {
    TableName: "DiscordAccounts",
    Key: { "DiscordId": String(userId) }
  };

  try {
    const data = await ddbDocClient.send(new GetCommand(params));
    console.log("DynamoDB Get Response:", JSON.stringify(data, null, 2));
    return data.Item || null;
  } catch (error) {
    console.error("Error getting user data:", error);
    return null;
  }
}

/**
 * @param {string} userId
 * @param {Object} userData
 * @returns {Promise<void>}
 */
async function saveUserData(userId, userData) {
  const params = {
    TableName: TABLE_NAME,
    Item: {
      DiscordId: userId,
      ...userData
    }
  };
  try {
    await ddbDocClient.send(new PutCommand(params));
  } catch (error) {
    console.error("Error saving user data to DynamoDB:", error);
    throw error;
  }
}

/**
 * @returns {Promise<Array>}
 */
/**
 * Retrieves all user data from DynamoDB, handling pagination.
 * @returns {Promise<Array>}
 */
async function listUserData() {
  const params = {
    TableName: TABLE_NAME
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

    return allItems;
  } catch (error) {
    console.error("Error listing user data from DynamoDB:", error);
    throw error;
  }
}

/**
 * @param {string} userId -
 * @returns {Promise<void>}
 */
async function incrementMessageLeaderWins(userId) {
  const params = {
    TableName: TABLE_NAME,
    Key: { DiscordId: userId },
    UpdateExpression: "SET messageLeaderWins = if_not_exists(messageLeaderWins, :zero) + :inc",
    ExpressionAttributeValues: {
      ":inc": 1,
      ":zero": 0
    }
  };
  try {
    await ddbDocClient.send(new UpdateCommand(params));
  } catch (error) {
    console.error("Error incrementing message leader wins:", error);
    throw error;
  }
}

module.exports = {
  getUserData,
  saveUserData,
  listUserData,
  incrementMessageLeaderWins
};