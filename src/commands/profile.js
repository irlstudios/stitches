const { SlashCommandBuilder, EmbedBuilder, ActionRowBuilder, ButtonBuilder, ButtonStyle } = require('discord.js');
const { getUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('profile')
      .setDescription("View your or another user's profile stats.")
      .addUserOption(option =>
          option.setName('target')
              .setDescription('The user whose profile you want to view')
              .setRequired(false)),

  async execute(interaction) {
    const { guild, user: interactionUser } = interaction;
    if (!guild) {
      return interaction.reply({ content: "This command can only be used in a server.", ephemeral: true });
    }

    const targetUser = interaction.options.getUser('target') || interactionUser;
    const targetMember = await guild.members.fetch(targetUser.id).catch(() => null);

    if (!targetMember) {
      if (targetUser.id !== interactionUser.id) {
        return interaction.reply({ content: "Could not find that user in this server.", ephemeral: true });
      }
      console.warn(`Could not fetch member object for interaction user ${interactionUser.id} in guild ${guild.id}`);
    }


    try {
      await interaction.deferReply({ ephemeral: true });

      const config = await getConfig(guild.id);
      if (!config) {
        return interaction.editReply({ content: 'Bot configuration is missing for this server. Please ask an admin to run `/setup-bot`.', ephemeral: true });
      }

      const userData = await getUserData(guild.id, targetUser.id);

      if (!userData) {
        return interaction.editReply({ content: `${targetUser.username} hasn't interacted with the bot systems yet and has no profile data.`, ephemeral: true });
      }

      const pages = generateProfilePages(targetUser, targetMember, userData, guild, config);

      if (!pages || pages.length === 0) {
        return interaction.editReply({ content: 'No relevant profile information found based on enabled systems.', ephemeral: true });
      }


      let currentPage = 0;
      const totalPages = pages.length;

      const getRow = (page, disabled = false) => getPaginationRowStatic(page, totalPages, disabled);

      const profileMessage = await interaction.editReply({
        embeds: [pages[currentPage]],
        components: totalPages > 1 ? [getRow(currentPage)] : [],
        fetchReply: true
      });

      if (totalPages <= 1) return;

      const filter = i => (i.customId === 'prev_profile' || i.customId === 'next_profile') && i.user.id === interactionUser.id;

      const collector = profileMessage.createMessageComponentCollector({ filter, time: 120000 });

      collector.on('collect', async i => {
        try {
          await i.deferUpdate();

          if (i.customId === 'prev_profile') {
            currentPage = Math.max(currentPage - 1, 0);
          } else if (i.customId === 'next_profile') {
            currentPage = Math.min(currentPage + 1, totalPages - 1);
          }
          await interaction.editReply({ embeds: [pages[currentPage]], components: [getRow(currentPage)] });
        } catch (updateError) {
          console.error("Error updating profile pagination:", updateError);
        }
      });

      collector.on('end', async (collected, reason) => {
        if (reason !== 'messageDelete' && reason !== 'channelDelete' && reason !== 'guildDelete') {
          try {
            await interaction.editReply({ components: [getRow(currentPage, true)] });
          } catch (endError) {
          }
        }
      });

    } catch (error) {
      console.error(`Error executing profile command for ${targetUser.id} in guild ${guild.id}:`, error);
      const errorMsg = 'An error occurred while loading the profile.';
      if (!interaction.replied && !interaction.deferred) {
        await interaction.reply({ content: errorMsg, ephemeral: true }).catch(console.error);
      } else {
        if (interaction.editable) {
          await interaction.editReply({ content: errorMsg, embeds: [], components: [] }).catch(console.error);
        }
      }
    }
  },
};



function generateProfilePages(user, member, userData, guild, config) {
  const pages = [];
  const safeUserData = userData || {};

  const displayColor = member?.displayHexColor || '#2ECC71';

  const baseEmbed = () => new EmbedBuilder()
      .setAuthor({ name: `${user.username}'s Profile`, iconURL: user.displayAvatarURL({ dynamic: true }) })
      .setThumbnail(user.displayAvatarURL({ dynamic: true, size: 128 }))
      .setColor(displayColor)
      .setTimestamp()
      .setFooter({ text: `User ID: ${user.id} | Guild: ${guild.name}` });

  const corePage = baseEmbed();
  corePage.setDescription("Core activity statistics.");
  let coreFieldsAdded = false;

  if (config.streakSystem?.enabled) {
    const currentStreak = safeUserData.streak ?? 0;
    const highestStreak = safeUserData.highestStreak ?? 0;
    const streakRole = getStreakRoleInfo(currentStreak, guild, config);
    const progressToNext = calculateProgressToNextMilestone(currentStreak, config);
    corePage.addFields(
        { name: 'Current Streak', value: `🔥 ${currentStreak} days`, inline: true },
        { name: 'Highest Streak', value: `🏆 ${highestStreak} days`, inline: true },
        { name: 'Streak Role', value: streakRole.name || 'None', inline: true },
        { name: 'Next Milestone', value: progressToNext || 'N/A', inline: false }
    );
    coreFieldsAdded = true;
  }

  if (config.levelSystem?.enabled) {
    const currentLevel = safeUserData.experience?.level ?? 0;
    const currentXp = safeUserData.experience?.totalXp ?? 0;
    const levelProgress = calculateLevelProgress(safeUserData, config);
    const levelRole = getLevelRoleInfo(currentLevel, guild, config);
    corePage.addFields(
        { name: 'Level', value: `⭐ ${currentLevel}`, inline: true },
        { name: 'XP', value: `${currentXp} XP`, inline: true },
        { name: 'Next Lvl In', value: levelProgress.xpToNext > 0 ? `${levelProgress.xpToNext} XP (${levelProgress.progressPercent}%)` : "Max?", inline: true },
        { name: 'Level Role', value: levelRole.name || 'None', inline: true },
    );
    coreFieldsAdded = true;
  }
  if (coreFieldsAdded) pages.push(corePage);

  const activityPage = baseEmbed();
  activityPage.setDescription("Engagement and activity details.");
  let activityFieldsAdded = false;

  const totalMessages = safeUserData.totalMessages ?? 0;
  const avgMessages = safeUserData.averageMessagesPerDay ?? 0;
  const activeDays = safeUserData.activeDaysCount ?? 0;
  const longestInactive = safeUserData.longestInactivePeriod ?? 0;
  const weeklyMessages = safeUserData.messages ?? 0;
  const consistency = calculateConsistencyRating(safeUserData);

  activityPage.addFields(
      { name: 'Messages (This Week)', value: `💬 ${weeklyMessages}`, inline: true},
      { name: 'Messages (Lifetime)', value: `✉️ ${totalMessages}`, inline: true },
      { name: 'Avg Msgs/Day', value: `📊 ${avgMessages.toFixed(2)}`, inline: true },
      { name: 'Active Days', value: `📅 ${activeDays}`, inline: true },
      { name: 'Longest Inactive', value: `⏳ ${longestInactive} days`, inline: true },
      { name: 'Consistency', value: `${consistency}`, inline: true}
  );
  activityFieldsAdded = true;

  if (config.messageLeaderSystem?.enabled) {
    const leaderWins = safeUserData.messageLeaderWins ?? 0;
    const consecutiveWins = safeUserData.mostConsecutiveLeader ?? 0;
    activityPage.addFields(
        { name: 'Msg Leader Wins', value: `🏅 ${leaderWins}`, inline: true },
        { name: 'Consecutive Wins', value: `✨ ${consecutiveWins}`, inline: true }
    );
    activityFieldsAdded = true;
  }

  if (activityFieldsAdded) pages.push(activityPage);

  const historyPage = baseEmbed();
  historyPage.setDescription("Achievements and historical data.");
  let historyFieldsAdded = false;

  if (config.streakSystem?.enabled && Array.isArray(safeUserData.milestones) && safeUserData.milestones.length > 0) {
    const formattedMilestones = safeUserData.milestones
        .slice(-5)
        .map(m => `> ${m.milestone}-day streak (<t:${Math.floor(new Date(m.date).getTime()/1000)}:d>)`)
        .join('\n');
    historyPage.addFields({ name: 'Recent Streak Milestones', value: formattedMilestones || 'None', inline: false });
    historyFieldsAdded = true;
  }

  if (Array.isArray(safeUserData.rolesAchieved) && safeUserData.rolesAchieved.length > 0) {
    const roleNames = safeUserData.rolesAchieved
        .map(roleId => guild.roles.cache.get(roleId)?.name)
        .filter(name => name)
        .slice(-5)
        .map(name => `> ${name}`)
        .join('\n');
    if (roleNames) {
      historyPage.addFields({ name: 'Recent Roles Earned', value: roleNames, inline: false });
      historyFieldsAdded = true;
    }
  }

  const lastLossTimestamp = safeUserData.lastStreakLoss ? Math.floor(new Date(safeUserData.lastStreakLoss).getTime() / 1000) : null;
  historyPage.addFields({ name: 'Last Streak Loss', value: lastLossTimestamp ? `<t:${lastLossTimestamp}:D>` : 'Never / N/A', inline: true });

  const lastUpdatedTimestamp = safeUserData.lastUpdated ? Math.floor(new Date(safeUserData.lastUpdated).getTime() / 1000) : null;
  historyPage.addFields({ name: 'Data Last Updated', value: lastUpdatedTimestamp ? `<t:${lastUpdatedTimestamp}:R>` : 'Unknown', inline: true });

  historyFieldsAdded = true;

  if (historyFieldsAdded) pages.push(historyPage);

  return pages;
}


function getPaginationRowStatic(currentPage, totalPages, disabled = false) {
  return new ActionRowBuilder().addComponents(
      new ButtonBuilder()
          .setCustomId('prev_profile')
          .setLabel('◀️')
          .setStyle(ButtonStyle.Primary)
          .setDisabled(disabled || currentPage === 0),
      new ButtonBuilder()
          .setCustomId('next_profile')
          .setLabel('▶️')
          .setStyle(ButtonStyle.Primary)
          .setDisabled(disabled || currentPage >= totalPages - 1)
  );
}

function getStreakRoleInfo(currentStreak, guild, config) {
  if (!config?.streakSystem?.enabled || !currentStreak || currentStreak <= 0) {
    return { id: null, name: 'None' };
  }
  let highestMetRoleId = null;
  let highestMetThreshold = 0;

  for (const key in config.streakSystem) {
    if (key.startsWith('role') && key.endsWith('day')) {
      const threshold = parseInt(key.replace('role', '').replace('day', ''), 10);
      const roleId = config.streakSystem[key];
      if (!isNaN(threshold) && roleId && currentStreak >= threshold && threshold > highestMetThreshold) {
        highestMetThreshold = threshold;
        highestMetRoleId = roleId;
      }
    }
  }

  if (highestMetRoleId) {
    const role = guild.roles.cache.get(highestMetRoleId);
    return { id: role?.id, name: role?.name || `Unknown Role (ID: ...${highestMetRoleId.slice(-4)})` };
  }
  return { id: null, name: 'None' };
}

function getLevelRoleInfo(currentLevel, guild, config) {
  if (!config?.levelSystem?.enabled || !currentLevel || currentLevel <= 0) {
    return { id: null, name: 'None' };
  }
  let highestMetRoleId = null;
  let highestMetLevel = 0;

  for (const key in config.levelSystem) {
    if (key.startsWith('roleLevel')) {
      const threshold = parseInt(key.replace('roleLevel', ''), 10);
      const roleId = config.levelSystem[key];
      if (!isNaN(threshold) && roleId && currentLevel >= threshold && threshold > highestMetLevel) {
        highestMetLevel = threshold;
        highestMetRoleId = roleId;
      }
    }
  }

  if (highestMetRoleId) {
    const role = guild.roles.cache.get(highestMetRoleId);
    return { id: role?.id, name: role?.name || `Unknown Role (ID: ...${highestMetRoleId.slice(-4)})` };
  }
  return { id: null, name: 'None' };
}

function calculateProgressToNextMilestone(currentStreak = 0, config) {
  if (!config?.streakSystem?.enabled) return "System Disabled";

  const milestones = Object.keys(config.streakSystem)
      .filter(key => key.startsWith('role') && key.endsWith('day') && config.streakSystem[key])
      .map(key => parseInt(key.replace('role', '').replace('day', ''), 10))
      .filter(days => !isNaN(days) && days > 0)
      .sort((a, b) => a - b);

  if (milestones.length === 0) return "No milestones set";

  const nextMilestone = milestones.find(milestone => milestone > currentStreak);

  if (!nextMilestone) return 'Max Milestone Reached!';

  const remaining = nextMilestone - currentStreak;
  return `${remaining} day${remaining > 1 ? 's' : ''} to ${nextMilestone}-day role`;
}


function calculateLevelProgress(userData, config) {
  if (!config?.levelSystem?.enabled) return { currentXp: 0, xpNeeded: 0, xpToNext: 0, progressPercent: 0 };

  const level = userData?.experience?.level ?? 0;
  const currentXp = userData?.experience?.totalXp ?? 0;
  const multiplier = config.levelSystem.levelMultiplier || 1.5;
  const baseXP = config.levelSystem.baseXp || 100;

  const xpNeeded = Math.floor(baseXP * Math.pow(multiplier, level));
  const xpToNext = Math.max(0, xpNeeded - currentXp);
  const progressPercent = xpNeeded > 0 ? Math.max(0, Math.min(100, Math.floor((currentXp / xpNeeded) * 100))) : (currentXp > 0 ? 100 : 0);

  return { currentXp, xpNeeded, xpToNext, progressPercent };
}


function calculateConsistencyRating(userData) {
  const tracked = userData?.daysTracked ?? 1;
  const active = userData?.activeDaysCount ?? 0;
  if (tracked <= 3) return "Tracking...";

  const ratio = active / tracked;

  if (ratio >= 0.9) return 'Very High 🟢';
  if (ratio >= 0.7) return 'High 🟢';
  if (ratio >= 0.5) return 'Medium 🟡';
  if (ratio >= 0.3) return 'Low 🟠';
  return 'Very Low 🔴';
}