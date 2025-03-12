const { SlashCommandBuilder, AttachmentBuilder } = require('discord.js');
const { DynamoDBClient } = require('@aws-sdk/client-dynamodb');
const { DynamoDBDocumentClient, QueryCommand } = require('@aws-sdk/lib-dynamodb');
const canvafy = require('canvafy');
const { getConfig } = require('../configManager');

const TABLE_NAME = 'DiscordAccounts';
const ATTRIBUTE_INDEX = "NameOfIndex";

const ddbClient = new DynamoDBClient({});
const ddbDocClient = DynamoDBDocumentClient.from(ddbClient);

module.exports = {
  data: new SlashCommandBuilder()
      .setName('leaderboard')
      .setDescription('Display the leaderboard')
      .addStringOption(option =>
          option.setName('type')
              .setDescription('The type of leaderboard to display')
              .setRequired(true)
              .addChoices(
                  { name: 'Streaks', value: 'streaks' },
                  { name: 'Messages', value: 'messages' },
                  { name: 'Highest Streak', value: 'highestStreak' },
                  { name: 'Message Leader Wins', value: 'messageLeaderWins' },
                  { name: 'Average Messages Per Day', value: 'averageMessagesPerDay' },
                  { name: 'Level', value: 'level' }
              )
      ),
  async execute(interaction) {
    try {
      const guildId = interaction.guild.id;
      const config = await getConfig(guildId);
      if (!config) {
        return interaction.reply({
          content: ':x: Error loading configuration file.',
          ephemeral: true,
        });
      }

      await interaction.deferReply({ ephemeral: true });

      const type = interaction.options.getString('type');
      const leaderboardItems = await queryByAttribute(type);

      if (!leaderboardItems || leaderboardItems.length === 0) {
        return interaction.editReply({
          content: `:x: No data found for **${type}**.`
        });
      }

      const currentMembers = await interaction.guild.members.fetch();
      const filteredItems = leaderboardItems.filter(item =>
          item.discordAccountId && currentMembers.has(item.discordAccountId)
      );

      if (filteredItems.length === 0) {
        return interaction.editReply({
          content: `:x: No members in this server have **${type}** data.`
        });
      }

      filteredItems.sort((a, b) => b.count - a.count);

      const top10 = filteredItems.slice(0, 10).map((item, index) => {
        const member = interaction.guild.members.cache.get(item.discordAccountId);
        return {
          top: index + 1,
          avatar: member ? member.user.displayAvatarURL({ format: 'png' }) : '',
          tag: member ? member.user.username : 'Unknown',
          score: item.count
        };
      });

      const scoreboard = await new canvafy.Top()
          .setOpacity(0.6)
          .setScoreMessage(`${type.charAt(0).toUpperCase() + type.slice(1)}:`)
          .setBackground(
              'image',
              'https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg'
          )
          .setColors({
            box: '#212121',
            username: '#ffffff',
            score: '#ffffff',
            firstRank: '#f7c716',
            secondRank: '#9e9e9e',
            thirdRank: '#94610f',
          })
          .setUsersData(top10)
          .build();

      const attachment = new AttachmentBuilder(scoreboard, { name: `leaderboard-${interaction.user.id}.png` });

      await interaction.editReply({
        content: `Here is the **${type}** leaderboard:`,
        files: [attachment],
      });
    } catch (error) {
      console.error(`❌ Error executing leaderboard command: ${error.message}`);
      if (!interaction.replied) {
        await interaction.editReply({
          content: `❌ An error occurred while processing the leaderboard command: ${error.message}`,
        });
      }
    }
  },
};

async function queryByAttribute(attributeValue) {
  try {
    const params = {
      TableName: TABLE_NAME,
      IndexName: ATTRIBUTE_INDEX,
      KeyConditionExpression: '#attr = :val',
      ExpressionAttributeNames: {
        '#attr': 'attribute'
      },
      ExpressionAttributeValues: {
        ':val': attributeValue
      }
    };
    const data = await ddbDocClient.send(new QueryCommand(params));
    return data.Items || [];
  } catch (error) {
    console.error(`Error querying by attribute "${attributeValue}":`, error);
    return [];
  }
}