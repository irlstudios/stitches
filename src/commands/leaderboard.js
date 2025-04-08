const { SlashCommandBuilder, AttachmentBuilder, EmbedBuilder } = require('discord.js');
const { queryLeaderboard } = require('../dynamoDB');
const canvafy = require('canvafy');
const { getConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('leaderboard')
      .setDescription('Display the leaderboard for various stats.')
      .addStringOption(option =>
          option.setName('type')
              .setDescription('The type of leaderboard to display')
              .setRequired(true)
              .addChoices(
                  { name: 'Streaks (Current)', value: 'streak' },
                  { name: 'Messages (Weekly)', value: 'messages' },
                  { name: 'Highest Streak (All Time)', value: 'highestStreak' },
                  { name: 'Message Leader Wins', value: 'messageLeaderWins' },
                  { name: 'XP (Total)', value: 'totalXp' },
                  { name: 'Level', value: 'level' },
                  { name: 'Active Days Count', value: 'activeDaysCount' },
                  { name: 'Avg Messages Per Day', value: 'averageMessagesPerDay' },
                  { name: 'Most Consecutive Leader Wins', value: 'mostConsecutiveLeader' }
              )
      )
      .addIntegerOption(option =>
          option.setName('limit')
              .setDescription('How many users to display (default 10, max 25)')
              .setMinValue(1)
              .setMaxValue(25)
              .setRequired(false)),

  async execute(interaction) {
    const { guild } = interaction;
    if (!guild) {
      return interaction.reply({ content: "This command can only be used in a server.", ephemeral: true });
    }

    const leaderboardType = interaction.options.getString('type');
    const limit = interaction.options.getInteger('limit') ?? 10;

    try {
      const config = await getConfig(guild.id);
      if (!config) {
        return interaction.reply({ content: ':x: Bot configuration missing for this server. Please run `/setup-bot` or contact an admin.', ephemeral: true });
      }

      let systemEnabled = true;
      switch (leaderboardType) {
        case 'streak':
        case 'highestStreak':
          systemEnabled = config.streakSystem?.enabled ?? false;
          break;
        case 'messages':
        case 'messageLeaderWins':
        case 'mostConsecutiveLeader':
          systemEnabled = config.messageLeaderSystem?.enabled ?? false;
          break;
        case 'level':
        case 'totalXp':
          systemEnabled = config.levelSystem?.enabled ?? false;
          break;
      }

      if (!systemEnabled && !['averageMessagesPerDay', 'activeDaysCount'].includes(leaderboardType)) {
        return interaction.reply({ content: `:warning: The system related to the "${leaderboardType}" leaderboard is currently disabled in this server's configuration.`, ephemeral: true });
      }


      await interaction.deferReply();
      const leaderboardItems = await queryLeaderboard(leaderboardType, guild.id, limit);

      if (!leaderboardItems || leaderboardItems.length === 0) {
        return interaction.editReply({
          content: `:x: No data found for the **${leaderboardType}** leaderboard in this server.`
        });
      }

      await guild.members.fetch();

      const validLeaderboardUsers = leaderboardItems
          .map(item => {
            const member = guild.members.cache.get(item.discordAccountId);
            if (!member) return null;
            return {
              userId: item.discordAccountId,
              username: member.user.username,
              avatar: member.user.displayAvatarURL({ format: 'png', dynamic: true, size: 128 }),
              score: item.count
            };
          })
          .filter(user => user !== null);

      if (validLeaderboardUsers.length === 0) {
        return interaction.editReply({
          content: `:x: No current members found with data for the **${leaderboardType}** leaderboard.`
        });
      }

      const canvafyUsers = validLeaderboardUsers.map((user, index) => ({
        top: index + 1,
        avatar: user.avatar,
        tag: user.username,
        score: Math.round(user.score * 100) / 100
      }));

      let scoreLabel = "Score:";
      switch(leaderboardType) {
        case 'streak': scoreLabel = 'Days:'; break;
        case 'messages': scoreLabel = 'Msgs:'; break;
        case 'highestStreak': scoreLabel = 'Days:'; break;
        case 'messageLeaderWins': scoreLabel = 'Wins:'; break;
        case 'totalXp': scoreLabel = 'XP:'; break;
        case 'level': scoreLabel = 'Lvl:'; break;
        case 'activeDaysCount': scoreLabel = 'Days:'; break;
        case 'averageMessagesPerDay': scoreLabel = 'Avg/Day:'; break;
        case 'mostConsecutiveLeader': scoreLabel = 'Wins:'; break;
      }

      const scoreboardImage = await new canvafy.Top()
          .setOpacity(0.7)
          .setScoreMessage(scoreLabel)
          .setBackground('image', 'https://img.freepik.com/premium-vector/red-fog-smoke-isolated-transparent-background-red-cloudiness-mist-smog-background-vector-realistic-illustration_221648-615.jpg')
          .setColors({ box: '#212121', username: '#ffffff', score: '#ffffff', firstRank: '#f7c716', secondRank: '#9e9e9e', thirdRank: '#94610f' })
          .setUsersData(canvafyUsers)
          .build();

      const attachment = new AttachmentBuilder(scoreboardImage, { name: `leaderboard-${guild.id}-${leaderboardType}.png` });

      const leaderboardTitle = leaderboardType
          .replace(/([A-Z])/g, ' $1')
          .replace(/^./, str => str.toUpperCase());

      await interaction.editReply({
        content: `🏆 **${leaderboardTitle} Leaderboard** for ${guild.name}`,
        files: [attachment],
      });

    } catch (error) {
      console.error(`❌ Error executing leaderboard command for type ${leaderboardType} in guild ${guild.id}:`, error);
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({ content: 'An error occurred while fetching the leaderboard.', ephemeral: true }).catch(console.error);
      } else {
        await interaction.editReply({ content: `❌ An error occurred while generating the leaderboard: ${error.message}` }).catch(console.error);
      }
    }
  },
};