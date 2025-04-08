const { Collection, StringSelectMenuBuilder, ActionRowBuilder, ChannelType } = require('discord.js');
const { set } = require('lodash');
const { getConfig, saveConfig, ensureConfigStructure } = require('./configManager');

const commandCooldowns = new Collection();

module.exports = async (client, interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      const command = client.commands.get(interaction.commandName);
      if (!command) {
        console.warn(`No command matching /${interaction.commandName} was found.`);
        return interaction.reply({
          content: `❓ Uh oh! I don't recognize the command \`/${interaction.commandName}\`. It might be outdated or removed.`,
          ephemeral: true,
        });
      }
      const now = Date.now();
      const cooldownAmount = (command.cooldown || 3) * 1000;
      const commandName = interaction.commandName;
      if (!commandCooldowns.has(commandName)) {
        commandCooldowns.set(commandName, new Collection());
      }
      const timestamps = commandCooldowns.get(commandName);
      const userTimestamp = timestamps.get(interaction.user.id);

      if (userTimestamp) {
        const expirationTime = userTimestamp + cooldownAmount;
        if (now < expirationTime) {
          const timeLeft = (expirationTime - now) / 1000;
          return interaction.reply({
            content: `⏳ Please wait ${timeLeft.toFixed(1)} more second(s) before reusing the \`/${commandName}\` command.`,
            ephemeral: true,
          });
        }
      }
      timestamps.set(interaction.user.id, now);
      setTimeout(() => timestamps.delete(interaction.user.id), cooldownAmount);


      try {
        await command.execute(interaction);
      } catch (error) {
        console.error(`Error executing command /${interaction.commandName}:`, error);
        await handleInteractionError(interaction, `💥 Oops! Something went wrong while executing the \`/${interaction.commandName}\` command.`);
      }

    } else if (interaction.isStringSelectMenu()) {
      const { guild, customId } = interaction;
      if (!guild) {
        return interaction.reply({
          content: "This select menu action is only available within a server.",
          ephemeral: true,
        });
      }

      let config;
      try {
        config = await getConfig(guild.id);
        if (!config) {
          if (interaction.isRepliable()) {
            await interaction.reply({
              content: '⚙️ Bot configuration is missing for this server. An admin needs to run `/setup-bot` first.',
              ephemeral: true,
            });
          }
          return;
        }
      } catch (error) {
        console.error(`Error loading configuration for guild ${guild.id} in select menu handler:`, error);
        if (interaction.isRepliable()) {
          await interaction.reply({
            content: 'An error occurred while loading server configuration.',
            ephemeral: true,
          });
        }
        return;
      }

      ensureConfigStructure(config);

      try {
        switch (customId) {
          case 'system-select':
            await handleSystemSelect(interaction, config);
            break;
          case 'streak-options':
            await handleStreakOptions(interaction, config);
            break;
          case 'leader-options':
            await handleLeaderOptions(interaction, config);
            break;
          case 'level-options':
            await handleLevelOptions(interaction, config);
            break;
          case 'report-options':
            await handleReportOptions(interaction, config);
            break;
          case (customId.startsWith('remove_streak_milestone_select_') ? customId : null):
          case (customId.startsWith('remove_level_milestone_select_') ? customId : null):
            if (!interaction.deferred && !interaction.replied) {
              await interaction.deferUpdate();
            }
            break;
          default:
            console.warn(`Unhandled select menu interaction with ID: ${customId} in guild ${guild.id}`);
            if (interaction.isRepliable()) {
              await interaction.reply({ content: "This menu selection is not currently recognized or has expired.", ephemeral: true });
            }
        }
      } catch (error) {
        console.error(`Error handling select menu interaction (${customId}) in guild ${guild.id}:`, error);
        await handleInteractionError(interaction, 'There was an error processing your selection.');
      }
    }
  } catch (error) {
    console.error('Critical error in interaction handler:', error);
    if (interaction.isRepliable()) {
      await handleInteractionError(interaction, 'A critical error occurred. Please try again later or contact support.');
    }
  }
};

async function handleInteractionError(interaction, errorMessage) {
  const payload = { content: errorMessage, ephemeral: true, embeds: [], components: [] };
  try {
    if (interaction.replied || interaction.deferred) {
      if (interaction.editable) {
        await interaction.editReply(payload).catch(console.warn);
      } else {
        await interaction.followUp(payload).catch(console.warn);
      }
    } else if (interaction.isRepliable()) {
      await interaction.reply(payload).catch(console.warn);
    }
  } catch (replyError) {
    console.error('Failed to send error reply to interaction:', replyError);
  }
}


async function handleSystemSelect(interaction, config) {
  if (!config) {
    console.error("handleSystemSelect called with null config.");
    return handleInteractionError(interaction, "Configuration error occurred.");
  }

  const systemKey = interaction.values[0];
  let menu, content;
  const { guild } = interaction;

  switch (systemKey) {
    case 'streakSystem':
      menu = new StringSelectMenuBuilder()
          .setCustomId('streak-options')
          .setPlaceholder('Streak System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewStreakConfig', description: "See current streak settings." },
            { label: 'Toggle System (On/Off)', value: 'toggleStreak', description: `${config.streakSystem?.enabled ? 'Disable' : 'Enable'} the streak system.` },
            { label: 'Set Output Channel', value: 'channelStreakOutput', description: 'Channel for streak up messages.' },
            { label: 'Set Streak Threshold', value: 'streakThreshold', description: 'Messages needed daily.' },
            { label: 'Add Streak Role Milestone', value: 'addMilestone', description: 'Add a role reward for X days.' },
            { label: 'Remove Streak Role Milestone', value: 'removeMilestone', description: 'Remove a streak role reward.' }
          ]);
      content = `Configure the **Streak System** (Currently: ${config.streakSystem?.enabled ? 'Enabled' : 'Disabled'}):`;
      break;

    case 'messageLeaderSystem':
      menu = new StringSelectMenuBuilder()
          .setCustomId('leader-options')
          .setPlaceholder('Message Leader System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewLeaderConfig', description: "See current message leader settings." },
            { label: 'Toggle System (On/Off)', value: 'toggleLeader', description: `${config.messageLeaderSystem?.enabled ? 'Disable' : 'Enable'} the leader system.` },
            { label: 'Set Announcement Channel', value: 'channelMessageLeader', description: 'Channel for weekly winners.' },
            { label: 'Set Winner Role', value: 'roleMessageLeader', description: 'Role assigned to the winner(s).'}
          ]);
      content = `Configure the **Message Leader System** (Currently: ${config.messageLeaderSystem?.enabled ? 'Enabled' : 'Disabled'}):`;
      break;

    case 'levelSystem':
      menu = new StringSelectMenuBuilder()
          .setCustomId('level-options')
          .setPlaceholder('Level System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewLevelConfig', description: "See current level settings." },
            { label: 'Toggle System (On/Off)', value: 'toggleLevel', description: `${config.levelSystem?.enabled ? 'Disable' : 'Enable'} the level system.` },
            { label: 'Set Level Up Channel', value: 'channelLevelUp', description: 'Channel for level up messages.' },
            { label: 'Set XP Per Message', value: 'xpPerMessage', description: 'XP gained per message.' },
            { label: 'Set Level Multiplier', value: 'levelMultiplier', description: 'Difficulty increase per level.' },
            { label: 'Toggle Level Up Messages', value: 'toggleLevelMsgs', description: `${config.levelSystem?.levelUpMessages ? 'Disable' : 'Enable'} level up pings.`},
            { label: 'Add Level Role Milestone', value: 'addLevelMilestone', description: 'Add a role reward for Level X.' },
            { label: 'Remove Level Role Milestone', value: 'removeLevelMilestone', description: 'Remove a level role reward.' },
          ]);
      content = `Configure the **Level System** (Currently: ${config.levelSystem?.enabled ? 'Enabled' : 'Disabled'}):`;
      break;

    case 'reportSettings':
      menu = new StringSelectMenuBuilder()
          .setCustomId('report-options')
          .setPlaceholder('Analytics System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewReportConfig', description: "See current report channels." },
            { label: 'Set Weekly Report Channel', value: 'weeklyReportChannel', description: 'Channel for Sunday reports.' },
            { label: 'Set Monthly Report Channel', value: 'monthlyReportChannel', description: 'Channel for monthly reports.' }
          ]);
      content = 'Configure **Analytics / Reports**:';
      break;

    default:
      console.warn(`Unhandled system selection in handleSystemSelect: ${systemKey}`);
      if (interaction.isRepliable()) await interaction.deferUpdate().catch(console.warn);
      return;
  }

  if (menu) {
    const row = new ActionRowBuilder().addComponents(menu);
    await interaction.update({ content, components: [row] });
  } else {
    console.error(`No menu generated for valid system key: ${systemKey}`);
    await interaction.update({ content: "Error generating configuration options.", components: [] });
  }
}

async function handleStreakOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;

  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information.");

  switch (option) {
    case 'viewStreakConfig': {
      const streakSystem = config.streakSystem || {};
      const milestones = Object.entries(streakSystem)
          .filter(([key, value]) => key.startsWith('role') && key.endsWith('day') && value)
          .map(([key, value]) => {
            const days = key.replace('role', '').replace('day', '');
            const roleName = guild.roles.cache.get(value)?.name || 'Unknown/Deleted Role';
            return `> ${days} Days: @${roleName}`;
          })
          .join('\n') || '> None set';
      const outputChannelId = streakSystem.channelStreakOutput;
      const outputChannelName = outputChannelId ? `<#${outputChannelId}>` : 'Not Set';
      const enabledText = streakSystem.enabled ? '✅ Yes' : '❌ No';
      const threshold = streakSystem.streakThreshold ?? 'Not Set';

      await interaction.reply({
        content: `**Streak System Config:**\nEnabled: ${enabledText}\nThreshold: \`${threshold}\` messages/day\nOutput Channel: ${outputChannelName}\nMilestones:\n${milestones}`,
        ephemeral: true,
      });
      break;
    }
    case 'toggleStreak':
      config.streakSystem.enabled = !config.streakSystem.enabled;
      await saveConfig(guildId, config);
      await interaction.update({ content: `✅ Streak System has been **${config.streakSystem.enabled ? 'enabled' : 'disabled'}**.`, components: [] });
      break;
    case 'channelStreakOutput':
      await setChannel(interaction, config, guildId, 'streakSystem.channelStreakOutput', 'Streak Output Channel');
      break;
    case 'streakThreshold':
      await setNumericValue(interaction, config, guildId, 'streakSystem.streakThreshold', 'Streak threshold (daily messages required)', { min: 1, max: 100, integer: true });
      break;
    case 'addMilestone':
      await addMilestone(interaction, config, guildId, 'streak');
      break;
    case 'removeMilestone':
      await removeMilestone(interaction, config, guildId, 'streak');
      break;
    default:
      console.warn(`Unknown streak option: ${option}`);
      await interaction.reply({ content: 'Unknown streak option selected.', ephemeral: true });
  }
}

async function handleLeaderOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;
  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information.");

  const leaderSystem = config.messageLeaderSystem || {};

  switch (option) {
    case 'viewLeaderConfig': {
      const channelId = leaderSystem.channelMessageLeader;
      const roleId = leaderSystem.roleMessageLeader;
      const channelName = channelId ? `<#${channelId}>` : 'Not Set';
      const roleName = roleId ? `<@&${roleId}>` : 'Not Set';
      const enabledText = leaderSystem.enabled ? '✅ Yes' : '❌ No';

      await interaction.reply({
        content: `**Message Leader System Config:**\nEnabled: ${enabledText}\nAnnouncement Channel: ${channelName}\nWinner Role: ${roleName}`,
        ephemeral: true,
      });
      break;
    }
    case 'toggleLeader':
      config.messageLeaderSystem.enabled = !leaderSystem.enabled;
      await saveConfig(guildId, config);
      await interaction.update({ content: `✅ Message Leader System has been **${config.messageLeaderSystem.enabled ? 'enabled' : 'disabled'}**.`, components: [] });
      break;
    case 'channelMessageLeader':
      await setChannel(interaction, config, guildId, 'messageLeaderSystem.channelMessageLeader', 'Message Leader Announcement Channel');
      break;
    case 'roleMessageLeader':
      await setRole(interaction, config, guildId, 'messageLeaderSystem.roleMessageLeader', 'Message Leader Winner Role');
      break;
    default:
      console.warn(`Unknown leader option: ${option}`);
      await interaction.reply({ content: 'Unknown leader option selected.', ephemeral: true });
  }
}

async function handleLevelOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;
  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information.");

  const levelSystem = config.levelSystem || {};

  switch(option) {
    case 'viewLevelConfig': {
      const milestones = Object.entries(levelSystem)
          .filter(([key, value]) => key.startsWith('roleLevel') && value)
          .map(([key, value]) => {
            const level = key.replace('roleLevel', '');
            const roleName = guild.roles.cache.get(value)?.name || 'Unknown/Deleted Role';
            return `> Level ${level}: @${roleName}`;
          })
          .join('\n') || '> None set';
      const channelId = levelSystem.channelLevelUp;
      const channelName = channelId ? `<#${channelId}>` : 'Current Channel';
      const enabledText = levelSystem.enabled ? '✅ Yes' : '❌ No';
      const pingsEnabledText = levelSystem.levelUpMessages ? '✅ Yes' : '❌ No';
      const xpPerMsg = levelSystem.xpPerMessage ?? 'Not Set';
      const multiplier = levelSystem.levelMultiplier ?? 'Not Set';

      await interaction.reply({
        content: `**Level System Config:**\nEnabled: ${enabledText}\nXP Per Message: \`${xpPerMsg}\`\nMultiplier: \`${multiplier}\`\nLevel Up Channel: ${channelName}\nLevel Up Pings: ${pingsEnabledText}\nMilestones:\n${milestones}`,
        ephemeral: true,
      });
      break;
    }
    case 'toggleLevel':
      config.levelSystem.enabled = !levelSystem.enabled;
      await saveConfig(guildId, config);
      await interaction.update({ content: `✅ Level System has been **${config.levelSystem.enabled ? 'enabled' : 'disabled'}**.`, components: [] });
      break;
    case 'channelLevelUp':
      await setChannel(interaction, config, guildId, 'levelSystem.channelLevelUp', 'Level-Up Message Channel');
      break;
    case 'xpPerMessage':
      await setNumericValue(interaction, config, guildId, 'levelSystem.xpPerMessage', 'XP per message', { min: 0, integer: true });
      break;
    case 'levelMultiplier':
      await setNumericValue(interaction, config, guildId, 'levelSystem.levelMultiplier', 'Level difficulty multiplier', { min: 1.01, max: 5 });
      break;
    case 'toggleLevelMsgs':
      config.levelSystem.levelUpMessages = !levelSystem.levelUpMessages;
      await saveConfig(guildId, config);
      await interaction.update({ content: `✅ Level Up Messages have been **${config.levelSystem.levelUpMessages ? 'enabled' : 'disabled'}**.`, components: [] });
      break;
    case 'addLevelMilestone':
      await addMilestone(interaction, config, guildId, 'level');
      break;
    case 'removeLevelMilestone':
      await removeMilestone(interaction, config, guildId, 'level');
      break;
    default:
      console.warn(`Unknown level option: ${option}`);
      await interaction.reply({ content: 'Unknown level option selected.', ephemeral: true });
  }
}

async function handleReportOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;
  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information.");

  const reportSettings = config.reportSettings || {};

  switch(option) {
    case 'viewReportConfig': {
      const weeklyChId = reportSettings.weeklyReportChannel;
      const monthlyChId = reportSettings.monthlyReportChannel;
      const weeklyCh = weeklyChId ? `<#${weeklyChId}>` : 'Not Set';
      const monthlyCh = monthlyChId ? `<#${monthlyChId}>` : 'Not Set';

      await interaction.reply({
        content: `**Analytics / Report Config:**\nWeekly Report Channel: ${weeklyCh}\nMonthly Report Channel: ${monthlyCh}`,
        ephemeral: true,
      });
      break;
    }
    case 'weeklyReportChannel':
      await setChannel(interaction, config, guildId, 'reportSettings.weeklyReportChannel', 'Weekly Report Channel');
      break;
    case 'monthlyReportChannel':
      await setChannel(interaction, config, guildId, 'reportSettings.monthlyReportChannel', 'Monthly Report Channel');
      break;
    default:
      console.warn(`Unknown report option: ${option}`);
      await interaction.reply({ content: 'Unknown report option selected.', ephemeral: true });
  }
}

async function setChannel(interaction, config, guildId, configKey, description) {
  await interaction.deferUpdate();
  await interaction.followUp({
    content: `Mention the text channel for **${description}**, or type \`clear\` to unset it.`,
    ephemeral: true, fetchReply: true
  });
  const filter = (msg) => msg.author.id === interaction.user.id && msg.guildId === guildId;
  const collector = interaction.channel.createMessageCollector({ filter, time: 30000, max: 1 });
  collector.on('collect', async (msg) => {
    try {
      let feedbackMsg = '';
      if (msg.content.toLowerCase() === 'clear') {
        set(config, configKey, null);
        feedbackMsg = `✅ **${description}** channel cleared.`;
      } else {
        const mentionedChannel = msg.mentions.channels.first();
        if (mentionedChannel && mentionedChannel.type === ChannelType.GuildText) {
          set(config, configKey, mentionedChannel.id);
          feedbackMsg = `✅ **${description}** set to ${mentionedChannel}.`;
        } else {
          await interaction.followUp({ content: '❌ Invalid input. Mention a text channel or type `clear`.', ephemeral: true });
          await msg.delete().catch(console.error);
          return collector.stop();
        }
      }
      await saveConfig(guildId, config);
      await interaction.followUp({ content: feedbackMsg, ephemeral: true });
      await msg.delete().catch(console.error);
    } catch (error) { console.error(`Error in setChannel collector for ${configKey}:`, error); await interaction.followUp({ content: 'Error setting channel.', ephemeral: true }); } finally { collector.stop(); }
  });
  collector.on('end', (c, r) => { if (r === 'time') interaction.followUp({ content: '⏰ Config cancelled (time out).', ephemeral: true }).catch(console.error); });
}

async function setNumericValue(interaction, config, guildId, configKey, description, options = {}) {
  const { min = 0, max = Infinity, integer = false } = options;
  await interaction.deferUpdate();
  await interaction.followUp({
    content: `Enter value for **${description}**.\n(Min: ${min}, Max: ${max === Infinity ? 'None' : max}${integer ? ', Whole numbers' : ''})`,
    ephemeral: true, fetchReply: true
  });
  const filter = (msg) => msg.author.id === interaction.user.id && msg.guildId === guildId;
  const collector = interaction.channel.createMessageCollector({ filter, time: 30000, max: 1 });
  collector.on('collect', async (msg) => {
    try {
      const value = integer ? parseInt(msg.content, 10) : parseFloat(msg.content);
      let feedbackMsg = '';
      if (isNaN(value) || value < min || value > max || (integer && !Number.isInteger(value))) {
        feedbackMsg = `❌ Invalid. Enter number between ${min}-${max === Infinity ? 'inf' : max}${integer ? ' (whole)' : ''}.`;
      } else {
        set(config, configKey, value);
        await saveConfig(guildId, config);
        feedbackMsg = `✅ **${description}** set to \`${value}\`.`;
      }
      await interaction.followUp({ content: feedbackMsg, ephemeral: true });
      await msg.delete().catch(console.error);
    } catch (error) { console.error(`Error in setNumericValue collector for ${configKey}:`, error); await interaction.followUp({ content: 'Error setting value.', ephemeral: true }); } finally { collector.stop(); }
  });
  collector.on('end', (c, r) => { if (r === 'time') interaction.followUp({ content: '⏰ Config cancelled (time out).', ephemeral: true }).catch(console.error); });
}

async function setRole(interaction, config, guildId, configKey, description) {
  await interaction.deferUpdate();
  await interaction.followUp({
    content: `Mention the role for **${description}**, or type \`clear\` to unset.`,
    ephemeral: true, fetchReply: true
  });
  const filter = (msg) => msg.author.id === interaction.user.id && msg.guildId === guildId;
  const collector = interaction.channel.createMessageCollector({ filter, time: 30000, max: 1 });
  collector.on('collect', async (msg) => {
    try {
      let feedbackMsg = '';
      if (msg.content.toLowerCase() === 'clear') {
        set(config, configKey, null);
        feedbackMsg = `✅ **${description}** role cleared.`;
      } else {
        const mentionedRole = msg.mentions.roles.first();
        if (mentionedRole) {
          if (mentionedRole.id === guildId) { feedbackMsg = '❌ Cannot use @everyone.'; /* Stop */ }
          else if (interaction.guild.members.me?.roles.highest.position <= mentionedRole.position) { feedbackMsg = `❌ Cannot manage ${mentionedRole} (role too high).`; /* Stop */ }
          else {
            set(config, configKey, mentionedRole.id);
            feedbackMsg = `✅ **${description}** set to ${mentionedRole}.`;
          }
        } else { feedbackMsg = '❌ Invalid. Mention a role or type `clear`.'; /* Stop */ }

        if (feedbackMsg.startsWith('❌')) {
          await interaction.followUp({ content: feedbackMsg, ephemeral: true });
          await msg.delete().catch(console.error);
          return collector.stop();
        }
      }
      await saveConfig(guildId, config);
      await interaction.followUp({ content: feedbackMsg, ephemeral: true });
      await msg.delete().catch(console.error);
    } catch (error) { console.error(`Error in setRole collector for ${configKey}:`, error); await interaction.followUp({ content: 'Error setting role.', ephemeral: true }); } finally { collector.stop(); }
  });
  collector.on('end', (c, r) => { if (r === 'time') interaction.followUp({ content: '⏰ Config cancelled (time out).', ephemeral: true }).catch(console.error); });
}

async function addMilestone(interaction, config, guildId, systemType) {
  await interaction.deferUpdate();
  await interaction.followUp({
    content: `Enter number for **${systemType} milestone** (e.g., 10):`,
    ephemeral: true, fetchReply: true
  });
  const filterNumber = msg => msg.author.id === interaction.user.id && msg.guildId === guildId;
  const numberCollector = interaction.channel.createMessageCollector({ filter: filterNumber, time: 30000, max: 1 });
  numberCollector.on('collect', async numberMsg => {
    try {
      const milestoneNumber = parseInt(numberMsg.content, 10);
      await numberMsg.delete().catch(console.error);
      if (isNaN(milestoneNumber) || milestoneNumber <= 0) { await interaction.followUp({ content: '❌ Invalid number.', ephemeral: true }); return numberCollector.stop(); }
      const configPath = systemType === 'streak' ? 'streakSystem' : 'levelSystem';
      const roleNamePrefix = systemType === 'streak' ? `${milestoneNumber} Day Streak` : `Level ${milestoneNumber}`;
      const configRoleKey = systemType === 'streak' ? `role${milestoneNumber}day` : `roleLevel${milestoneNumber}`;
      if (config[configPath]?.[configRoleKey]) { await interaction.followUp({ content: `⚠️ Milestone for ${milestoneNumber} already exists. Remove it first.`, ephemeral: true }); return numberCollector.stop(); }

      await interaction.followUp({ content: `Mention role for **${roleNamePrefix}**, type 'create', or 'cancel'.`, ephemeral: true, fetchReply: true });
      const filterRole = msg => msg.author.id === interaction.user.id && msg.guildId === guildId;
      const roleCollector = interaction.channel.createMessageCollector({ filter: filterRole, time: 45000, max: 1 });
      roleCollector.on('collect', async roleMsg => {
        try {
          let targetRole = null, feedbackMsg = '', roleContentLower = roleMsg.content.toLowerCase();
          if (roleContentLower === 'cancel') { feedbackMsg = 'Cancelled.'; }
          else if (roleContentLower === 'create') {
            try { targetRole = await interaction.guild.roles.create({ name: roleNamePrefix, reason: `Milestone role` }); feedbackMsg = `✅ Created & set ${targetRole}.`; }
            catch (e) { feedbackMsg = `❌ Failed to create role: ${e.message}`; }
          } else {
            targetRole = roleMsg.mentions.roles.first();
            if (!targetRole) { feedbackMsg = '❌ No valid role mentioned.'; }
            else if (targetRole.id === guildId) { feedbackMsg = '❌ Cannot use @everyone.'; targetRole = null; }
            else if (interaction.guild.members.me?.roles.highest.position <= targetRole.position) { feedbackMsg = `❌ Cannot manage ${targetRole} (role too high).`; targetRole = null; }
            else { feedbackMsg = `✅ Role ${targetRole} selected.`; }
          }
          if (targetRole) { set(config, `${configPath}.${configRoleKey}`, targetRole.id); await saveConfig(guildId, config); }
          await interaction.followUp({ content: feedbackMsg, ephemeral: true });
          await roleMsg.delete().catch(console.error);
        } catch (e) { console.error("Err adding milestone role:", e); await interaction.followUp({ content: 'Error setting role.', ephemeral: true }); } finally { roleCollector.stop(); }
      });
      roleCollector.on('end', (c, r) => { if (r === 'time') interaction.followUp({ content: '⏰ Role selection timed out.', ephemeral: true }).catch(console.error); });
    } catch (e) { console.error("Err adding milestone num:", e); await interaction.followUp({ content: 'Error processing number.', ephemeral: true }); } finally { numberCollector.stop(); }
  });
  numberCollector.on('end', (c, r) => { if (r === 'time') interaction.followUp({ content: '⏰ Number input timed out.', ephemeral: true }).catch(console.error); });
}

async function removeMilestone(interaction, config, guildId, systemType) {
  await interaction.deferUpdate();
  const configPath = systemType === 'streak' ? 'streakSystem' : 'levelSystem';
  const milestones = Object.entries(config[configPath] ?? {})
      .map(([key, roleId]) => {
        const match = key.match(/\d+$/); if (!match || !roleId) return null;
        const number = parseInt(match[0], 10); const roleName = interaction.guild.roles.cache.get(roleId)?.name || 'Deleted Role';
        return { number, roleId, roleName, key };
      }).filter(m => m !== null).sort((a, b) => a.number - b.number);
  if (milestones.length === 0) { return interaction.followUp({ content: `No ${systemType} milestones to remove.`, ephemeral: true }); }
  const options = milestones.map(m => ({ label: `${systemType==='streak'?m.number+' Days': 'Level '+m.number}`, description: `Role: @${m.roleName}`, value: m.key }));
  const selectMenu = new StringSelectMenuBuilder().setCustomId(`remove_${systemType}_milestone_select_${interaction.id}`).setPlaceholder(`Select ${systemType} milestone to remove`).addOptions(options.slice(0, 25));
  const row = new ActionRowBuilder().addComponents(selectMenu);
  const removalPrompt = await interaction.followUp({ content: `Select **${systemType} milestone** to remove:`, components: [row], ephemeral: true, fetchReply: true });
  const filter = i => i.customId === `remove_${systemType}_milestone_select_${interaction.id}` && i.user.id === interaction.user.id;
  const collector = removalPrompt.createMessageComponentCollector({ filter, time: 30000, max: 1 });
  collector.on('collect', async i => {
    try {
      await i.deferUpdate(); const selectedKey = i.values[0];
      const milestoneToRemove = milestones.find(m => m.key === selectedKey);
      if (!milestoneToRemove) { await interaction.editReply({ content: 'Invalid selection.', components: [] }); return; }
      if (config[configPath]?.[selectedKey]) {
        delete config[configPath][selectedKey]; await saveConfig(guildId, config);
        await interaction.editReply({ content: `✅ Removed ${milestoneToRemove.number} ${systemType==='streak'?'day':'level'} milestone (Role: @${milestoneToRemove.roleName}). Role **not** deleted.`, components: [] });
      } else { await interaction.editReply({ content: 'Milestone not found in config.', components: [] }); }
    } catch (e) { console.error(`Err removing milestone ${i.values[0]}:`, e); await interaction.editReply({ content: 'Error removing milestone.', components: [] }); } finally { collector.stop(); }
  });
  collector.on('end', (c, r) => { if (r === 'time') interaction.editReply({ content: '⏰ Removal cancelled (time out).', components: [] }).catch(console.error); });
}