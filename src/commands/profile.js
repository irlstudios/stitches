const { SlashCommandBuilder, EmbedBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle } = require('discord.js');
const { getUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('profile')
      .setDescription('View your profile data.')
      .addUserOption(option =>
          option.setName('target')
              .setDescription('The user whose profile you want to view')
              .setRequired(false)),
  async execute(interaction) {
    const targetUser = interaction.options.getUser('target') || interaction.user;
    const guildId = interaction.guild.id;
    const userId = targetUser.id;

    let config, userData;
    try {
      config = await getConfig(guildId);
      if (!config) {
        return interaction.reply({ content: 'An error occurred: configuration not found.', ephemeral: true });
      }
      userData = await getUserData(userId);
    } catch (error) {
      console.error(`Failed to load config or user data: ${error}`);
      return interaction.reply({ content: 'An error occurred while loading the profile data.', ephemeral: true });
    }

    if (!userData) {
      return interaction.reply({ content: `${targetUser.username} has no profile data.`, ephemeral: true });
    }

    const pages = generateProfilePages(targetUser, userData, interaction.guild, config);
    let currentPage = 0;

    try {
      await interaction.deferReply({ ephemeral: true });
      const profileEmbed = await interaction.editReply({
        embeds: [pages[currentPage]],
        components: [getPaginationRow(currentPage, pages.length)],
        fetchReply: true
      });

      const collector = profileEmbed.createMessageComponentCollector({
        filter: (btnInteraction) => btnInteraction.user.id === interaction.user.id,
        time: 60000
      });

      collector.on('collect', async (btnInteraction) => {
        if (btnInteraction.customId === 'next') {
          currentPage = Math.min(currentPage + 1, pages.length - 1);
        } else if (btnInteraction.customId === 'prev') {
          currentPage = Math.max(currentPage - 1, 0);
        }
        await btnInteraction.update({
          embeds: [pages[currentPage]],
          components: [getPaginationRow(currentPage, pages.length)]
        });
      });

      collector.on('end', async () => {
        await interaction.editReply({ components: [] });
      });
    } catch (err) {
      console.error(`Error sending profile interaction: ${err}`);
      return interaction.reply({ content: 'An error occurred while trying to display your profile.', ephemeral: true });
    }
  },
};

function generateProfilePages(user, userData, guild, config) {
  const pages = [];
  const baseEmbed = new EmbedBuilder()
      .setTitle(`${user.username}'s Profile`)
      .setThumbnail(user.displayAvatarURL({ dynamic: true }))
      .setColor('#2ECC71');

  const streakRole = getStreakRole(userData.streak, userData.highestStreak, guild, config);
  const progressToNextMilestone = calculateProgressToNextMilestone(userData.streak, config);
  const consistencyRating = calculateConsistencyRating(userData);
  const levelProgress = calculateLevelProgress(userData);

  const uniqueMilestones = Array.isArray(userData.milestones)
      ? Array.from(new Set(userData.milestones.map(m => `${m.milestone}-day streak on ${new Date(m.date).toLocaleDateString()}`)))
      : [];
  const uniqueRoles = Array.isArray(userData.rolesAchieved)
      ? Array.from(new Set(userData.rolesAchieved))
      : [];

  const sections = [];

  if (config.streakSystem?.enabled) {
    sections.push({
      title: 'Streak Information',
      fields: [
        { name: 'Current Streak', value: `${userData.streak}`, inline: true },
        { name: 'Highest Streak', value: `${userData.highestStreak}`, inline: true },
        { name: 'Streak Role', value: streakRole.current || 'None', inline: true },
        { name: 'Next Milestone', value: progressToNextMilestone, inline: true },
        { name: 'Active Days', value: `${userData.activeDaysCount || 0}`, inline: true },
        { name: 'Longest Inactive Period', value: `${userData.longestInactivePeriod || 0} days`, inline: true },
        { name: 'Consistency Rating', value: consistencyRating, inline: true },
      ],
    });
  }

  if ((Array.isArray(userData.milestones) && userData.milestones.length) || (Array.isArray(userData.rolesAchieved) && userData.rolesAchieved.length)) {
    sections.push({
      title: 'Achievements',
      fields: [
        { name: 'Milestones Achieved', value: uniqueMilestones.join('\n') || 'None', inline: true },
        { name: 'Roles Achieved', value: uniqueRoles.join('\n') || 'None', inline: true },
        { name: 'Last Streak Loss', value: userData.lastStreakLoss ? new Date(userData.lastStreakLoss).toLocaleDateString() : 'Never', inline: true },
      ],
    });
  }

  if (config.messageLeaderSystem?.enabled) {
    sections.push({
      title: 'Message Leader Wins',
      fields: [
        { name: 'Message Leader Wins', value: `${userData.messageLeaderWins}`, inline: true },
        { name: 'Most Consecutive Wins', value: `${userData.mostConsecutiveLeader}`, inline: true },
      ],
    });
  }

  if (config.levelSystem?.enabled) {
    sections.push({
      title: 'Level and XP',
      fields: [
        { name: 'Current Level', value: `${userData.experience.level || 0}`, inline: true },
        { name: 'Current XP', value: `${userData.experience.totalXp || 0}`, inline: true },
        { name: 'XP to Next Level', value: levelProgress, inline: true },
      ],
    });
  }

  sections.push({
    title: 'Message Activity',
    fields: [
      { name: 'Total Messages', value: `${userData.totalMessages}`, inline: true },
      { name: 'Average Messages/Day', value: `${(userData.averageMessagesPerDay || 0).toFixed(2)}`, inline: true },
      { name: 'Messages in Current Week', value: `${Array.isArray(userData.messageHeatmap) ? userData.messageHeatmap.length : 0}`, inline: true },
    ],
  });

  sections.forEach(section => {
    const embed = new EmbedBuilder(baseEmbed);
    embed.setDescription(`**${section.title}**`);
    embed.addFields(section.fields);
    pages.push(embed);
  });
  return pages;
}

function getPaginationRow(currentPage, totalPages) {
  return new ActionRowBuilder().addComponents(
      new ButtonBuilder()
          .setCustomId('prev')
          .setLabel('◀️ Previous')
          .setStyle(ButtonStyle.Primary)
          .setDisabled(currentPage === 0),
      new ButtonBuilder()
          .setCustomId('next')
          .setLabel('Next ▶️')
          .setStyle(ButtonStyle.Primary)
          .setDisabled(currentPage === totalPages - 1)
  );
}

function getStreakRole(currentStreak, highestStreak, guild, config) {
  const streakRoles = {
    1: config.streakSystem.role1day,
    2: config.streakSystem.role2day,
    4: config.streakSystem.role4day,
    5: config.streakSystem.role5day,
  };

  let currentRole = null;
  let highestRole = null;
  for (const [threshold, roleId] of Object.entries(streakRoles)) {
    if (currentStreak >= threshold) {
      currentRole = guild.roles.cache.get(roleId)?.name;
    }
    if (highestStreak >= threshold) {
      highestRole = guild.roles.cache.get(roleId)?.name;
    }
  }
  return { current: currentRole || 'None', highest: highestRole || 'None' };
}

function calculateProgressToNextMilestone(currentStreak, config) {
  const milestones = Object.keys(config.streakSystem)
      .filter(key => key.startsWith('role'))
      .map(key => {
        const match = key.match(/\d+/);
        return match ? parseInt(match[0], 10) : 0;
      });
  milestones.sort((a, b) => a - b);
  const nextMilestone = milestones.find(milestone => milestone > currentStreak);
  if (!nextMilestone) return 'Max Milestone Achieved';
  const remaining = nextMilestone - currentStreak;
  return `${remaining} day${remaining > 1 ? 's' : ''} to next milestone (${nextMilestone} days).`;
}

function calculateConsistencyRating(userData) {
  const ratio = (userData.streak || 0) / (userData.daysTracked || 1);
  if (ratio > 0.8) return 'High';
  if (ratio > 0.5) return 'Medium';
  return 'Low';
}

function calculateLevelProgress(userData) {
  const level = userData.experience?.level || 0;
  const xp = userData.experience?.totalXp || 0;
  const xpRequiredForNextLevel = Math.floor(5 * Math.pow(level, 2) + 50 * level + 100);
  const remainingXP = xpRequiredForNextLevel - xp;
  return `${remainingXP} XP to reach level ${level + 1}`;
}