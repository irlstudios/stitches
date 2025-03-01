const { SlashCommandBuilder, AttachmentBuilder } = require('discord.js');
const canvafy = require('canvafy');
const { listUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

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
                  { name: 'Highest Message Count', value: 'highestMessageCount' },
                  { name: 'Most Consecutive Leader Wins', value: 'mostConsecutiveLeader' },
                  { name: 'Average Messages Per Day', value: 'averageMessagesPerDay' },
                  { name: 'Levels', value: 'levels' }
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

      const userDataArray = await listUserData();

      if (!userDataArray || userDataArray.length === 0) {
        return interaction.editReply({
          content: ':x: No user data available.',
        });
      }

      const currentMembers = await interaction.guild.members.fetch();

      let userArray = userDataArray
          .filter(user => user.DiscordId && currentMembers.has(user.DiscordId))
          .map(user => ({
            userId: user.DiscordId,
            userData: user
          }))
          .filter(({ userData }) => userData.messages > 0);

      if (userArray.length === 0) {
        return interaction.editReply({
          content: ':x: There are no users in the database yet to display!',
        });
      }

      const type = interaction.options.getString('type');
      let dataLabel;
      let userArrayForType;

      switch (type) {
        case 'streaks':
          if (!config.streakSystem?.enabled) {
            return interaction.editReply({
              content: ':x: The Streaks system is not enabled in this server.',
            });
          }
          dataLabel = 'Streaks:';
          userArrayForType = userArray.map(({ userId, userData }) => ({
            userId,
            value: userData.streak,
          })).filter(user => user.value > 0);
          break;
        case 'messages':
          if (!config.messageLeaderSystem?.enabled) {
            return interaction.editReply({
              content: ':x: The Message Leader system is not enabled in this server.',
            });
          }
          dataLabel = 'Messages:';
          userArrayForType = userArray.map(({ userId, userData }) => ({
            userId,
            value: userData.messages,
          })).filter(user => user.value > 0);
          break;
        case 'highestMessageCount':
          dataLabel = 'Highest Message Count:';
          userArrayForType = userArray.map(({ userId, userData }) => ({
            userId,
            value: userData.totalMessages || 0,
          })).filter(user => user.value > 0);
          break;
        case 'mostConsecutiveLeader':
          dataLabel = 'Most Consecutive Leader Wins:';
          userArrayForType = userArray.map(({ userId, userData }) => ({
            userId,
            value: userData.mostConsecutiveLeader || 0,
          })).filter(user => user.value > 0);
          break;
        case 'averageMessagesPerDay':
          dataLabel = 'Average Messages Per Day:';
          userArrayForType = userArray.map(({ userId, userData }) => ({
            userId,
            value: userData.daysTracked > 0 ? Math.round(userData.messages / userData.daysTracked) : 0,
          })).filter(user => user.value > 0);
          break;
        case 'levels':
          if (!config.levelSystem?.enabled) {
            return interaction.editReply({
              content: ':x: The Level system is not enabled in this server.',
            });
          }
          dataLabel = 'Levels:';
          userArrayForType = userArray.map(({ userId, userData }) => ({
            userId,
            value: userData.experience.level || 0,
          })).filter(user => user.value > 0);
          break;
        default:
          return interaction.editReply({
            content: ':x: Invalid leaderboard type selected.',
          });
      }

      if (userArrayForType.length === 0) {
        return interaction.editReply({
          content: ':x: There are no users with relevant data for this leaderboard.',
        });
      }

      userArrayForType.sort((a, b) => b.value - a.value);

      const top10Users = userArrayForType.slice(0, 10).map((user, index) => {
        const member = interaction.guild.members.cache.get(user.userId);
        return {
          top: index + 1,
          avatar: member ? member.user.displayAvatarURL({ format: 'png' }) : '',
          tag: member ? member.user.username : 'Unknown',
          score: user.value,
        };
      });

      const top = await new canvafy.Top()
          .setOpacity(0.6)
          .setScoreMessage(dataLabel)
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
          .setUsersData(top10Users)
          .build();

      const attachment = new AttachmentBuilder(top, { name: `top-${interaction.member.id}.png` });

      await interaction.editReply({
        content: "Here's your private leaderboard:",
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