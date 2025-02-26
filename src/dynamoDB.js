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
    TableName: TABLE_NAME,
    Key: {
      DiscordId: userId
    }
  };
  try {
    const { Item } = await ddbDocClient.send(new GetCommand(params));
    return Item ? Item : null;
  } catch (error) {
    console.error("Error retrieving user data from DynamoDB:", error);
    throw error;
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
async function listUserData() {
  const params = {
    TableName: TABLE_NAME
  };
  try {
    const { Items } = await ddbDocClient.send(new ScanCommand(params));
    return Items || [];
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