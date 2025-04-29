const { Collection, StringSelectMenuBuilder, ActionRowBuilder, ChannelType, PermissionsBitField } = require('discord.js');
const { set } = require('lodash');
const { getConfig, saveConfig, ensureConfigStructure } = require('./configManager');
const { safeParseNumber } = require('./dynamoDB');

const commandCooldowns = new Collection();

module.exports = async (client, interaction) => {
  try {
    if (interaction.isChatInputCommand()) {
      const command = client.commands.get(interaction.commandName);
      if (!command) {
        console.warn(`[Interaction Handler] No command matching /${interaction.commandName} was found.`);
        return interaction.reply({
          content: `❓ Uh oh! I don't recognize the command \`/${interaction.commandName}\`. It might be outdated or removed. Please check available commands.`,
          ephemeral: true,
        }).catch(e => console.error(`[Interaction Handler] Failed to send 'command not found' reply for /${interaction.commandName}:`, e));
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
          }).catch(e => console.error(`[Interaction Handler] Failed to send cooldown reply for /${interaction.commandName}:`, e));
        }
      }
      timestamps.set(interaction.user.id, now);
      setTimeout(() => timestamps.delete(interaction.user.id), cooldownAmount);


      try {
        await command.execute(interaction);
      } catch (error) {
        console.error(`[Interaction Handler] Error executing command /${interaction.commandName} for user ${interaction.user.id} in guild ${interaction.guildId}:`, error);
        await handleInteractionError(interaction, `💥 Oops! Something went wrong while executing the \`/${interaction.commandName}\` command. The error has been logged.`);
      }

    } else if (interaction.isStringSelectMenu()) {
      const { guild, customId } = interaction;
      if (!guild) {
        console.warn(`[Interaction Handler] Select menu interaction ${customId} received outside of a guild.`);
        return interaction.reply({
          content: "This select menu action is only available within a server.",
          ephemeral: true,
        }).catch(e => console.error(`[Interaction Handler] Failed to send 'guild only' reply for select menu ${customId}:`, e));
      }

      let config;
      try {
        config = await getConfig(guild.id);
        if (!config) {
          console.error(`[Interaction Handler] Configuration is missing for guild ${guild.id} during select menu handling (${customId}).`);
          if (interaction.isRepliable()) {
            await interaction.reply({
              content: '⚙️ Bot configuration is missing for this server. An admin needs to run `/setup-bot` or `/stitches-configuration` first.',
              ephemeral: true,
            }).catch(e => console.error(`[Interaction Handler] Failed to send 'config missing' reply for select menu ${customId}:`, e));
          }
          return;
        }
      } catch (error) {
        console.error(`[Interaction Handler] Error loading configuration for guild ${guild.id} in select menu handler (${customId}):`, error);
        if (interaction.isRepliable()) {
          await interaction.reply({
            content: 'An error occurred while loading server configuration. Please try again.',
            ephemeral: true,
          }).catch(e => console.error(`[Interaction Handler] Failed to send 'config load error' reply for select menu ${customId}:`, e));
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
              await interaction.deferUpdate().catch(e => console.warn(`[Interaction Handler] Failed deferUpdate on remove milestone select ${customId}:`, e));
            }

            break;
          default:
            console.warn(`[Interaction Handler] Unhandled select menu interaction with ID: ${customId} in guild ${guild.id}`);
            if (interaction.isRepliable() && !interaction.replied && !interaction.deferred) {
              await interaction.reply({ content: "This menu selection is not currently recognized or may have expired.", ephemeral: true }).catch(e => console.error(`[Interaction Handler] Failed to send 'unhandled menu' reply for ${customId}:`, e));
            } else if (interaction.isRepliable()) {
              await interaction.followUp({ content: "This menu selection is not currently recognized or may have expired.", ephemeral: true }).catch(e => console.error(`[Interaction Handler] Failed to send 'unhandled menu' followUp for ${customId}:`, e));
            }
        }
      } catch (error) {
        console.error(`[Interaction Handler] Error handling select menu logic (${customId}) in guild ${guild.id}:`, error);
        await handleInteractionError(interaction, 'There was an error processing your selection. The error has been logged.');
      }
    }
  } catch (error) {
    console.error('[Interaction Handler] Critical error at the top level of interaction handler:', error);
    if (interaction.isRepliable()) {
      await handleInteractionError(interaction, 'A critical error occurred while handling your request. Please try again later or contact support if the issue persists.');
    }
  }
};

async function handleInteractionError(interaction, errorMessage) {
  console.log(`[Interaction Error Handler] Sending error message: "${errorMessage}" for interaction ID ${interaction.id} (Type: ${interaction.type}, Command: ${interaction.commandName || interaction.customId})`);
  const payload = { content: errorMessage, ephemeral: true, embeds: [], components: [] };
  try {
    if (interaction.replied || interaction.deferred) {
      if (interaction.editable) {
        console.log(`[Interaction Error Handler] Attempting editReply for interaction ${interaction.id}.`);
        await interaction.editReply(payload).catch(e => console.error(`[Interaction Error Handler] Failed to editReply for interaction ${interaction.id}:`, e));
      } else {
        console.log(`[Interaction Error Handler] Interaction ${interaction.id} was not editable, attempting followUp.`);
        await interaction.followUp(payload).catch(e => console.error(`[Interaction Error Handler] Failed to followUp for interaction ${interaction.id}:`, e));
      }
    } else if (interaction.isRepliable()) {
      console.log(`[Interaction Error Handler] Attempting reply for interaction ${interaction.id}.`);
      await interaction.reply(payload).catch(e => console.error(`[Interaction Error Handler] Failed to reply for interaction ${interaction.id}:`, e));
    } else {
      console.warn(`[Interaction Error Handler] Interaction ${interaction.id} was not repliable, deferred, or replied to. Cannot send error message.`);
    }
  } catch (replyError) {
    console.error(`[Interaction Error Handler] Unexpected error within handleInteractionError itself while trying to send message for interaction ${interaction.id}:`, replyError);
  }
}


async function handleSystemSelect(interaction, config) {
  if (!config) {
    console.error("[Interaction Handler] handleSystemSelect called with null config.");
    return handleInteractionError(interaction, "Configuration error occurred. Cannot proceed.");
  }

  const systemKey = interaction.values[0];
  let menu, content;
  const { guild } = interaction;
  if (!guild) {
    return handleInteractionError(interaction, "Could not resolve guild information. Cannot proceed.");
  }

  console.log(`[Config] User ${interaction.user.id} selected system '${systemKey}' in guild ${guild.id}`);

  switch (systemKey) {
    case 'streakSystem':
      menu = new StringSelectMenuBuilder()
          .setCustomId('streak-options')
          .setPlaceholder('Streak System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewStreakConfig', description: "See current streak settings." },
            { label: 'Toggle System (On/Off)', value: 'toggleStreak', description: `${config.streakSystem?.enabled ? 'Disable' : 'Enable'} the streak system.` },
            { label: 'Set Output Channel', value: 'channelStreakOutput', description: 'Channel for streak up messages.' },
            { label: 'Set Streak Threshold', value: 'streakThreshold', description: 'Messages needed daily (Default: 10).' },
            { label: 'Add Streak Role Milestone', value: 'addMilestone', description: 'Add a role reward for reaching X days.' },
            { label: 'Remove Streak Role Milestone', value: 'removeMilestone', description: 'Remove an existing streak role reward.' }
          ]);
      content = `Configure the **Streak System** (Currently: ${config.streakSystem?.enabled ? '✅ Enabled' : '❌ Disabled'}):`;
      break;

    case 'messageLeaderSystem':
      menu = new StringSelectMenuBuilder()
          .setCustomId('leader-options')
          .setPlaceholder('Message Leader System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewLeaderConfig', description: "See current message leader settings." },
            { label: 'Toggle System (On/Off)', value: 'toggleLeader', description: `${config.messageLeaderSystem?.enabled ? 'Disable' : 'Enable'} the leader system.` },
            { label: 'Set Announcement Channel', value: 'channelMessageLeader', description: 'Channel for weekly winner announcements.' },
            { label: 'Set Winner Role', value: 'roleMessageLeader', description: 'Role assigned to the weekly winner(s).'}
          ]);
      content = `Configure the **Message Leader System** (Currently: ${config.messageLeaderSystem?.enabled ? '✅ Enabled' : '❌ Disabled'}):`;
      break;

    case 'levelSystem':
      menu = new StringSelectMenuBuilder()
          .setCustomId('level-options')
          .setPlaceholder('Level System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewLevelConfig', description: "See current level settings." },
            { label: 'Toggle System (On/Off)', value: 'toggleLevel', description: `${config.levelSystem?.enabled ? 'Disable' : 'Enable'} the level system.` },
            { label: 'Set Level Up Channel', value: 'channelLevelUp', description: 'Channel for level up messages (or current).' },
            { label: 'Set XP Per Message', value: 'xpPerMessage', description: 'XP gained per message (Default: 10).' },
            { label: 'Set Level Multiplier', value: 'levelMultiplier', description: 'Difficulty increase per level (Default: 1.5).' },
            { label: 'Toggle Level Up Messages', value: 'toggleLevelMsgs', description: `${config.levelSystem?.levelUpMessages !== false ? 'Disable' : 'Enable'} level up pings.`},
            { label: 'Add Level Role Milestone', value: 'addLevelMilestone', description: 'Add a role reward for reaching Level X.' },
            { label: 'Remove Level Role Milestone', value: 'removeLevelMilestone', description: 'Remove an existing level role reward.' },
          ]);
      content = `Configure the **Level System** (Currently: ${config.levelSystem?.enabled ? '✅ Enabled' : '❌ Disabled'}):`;
      break;

    case 'reportSettings':
      menu = new StringSelectMenuBuilder()
          .setCustomId('report-options')
          .setPlaceholder('Analytics System Options')
          .addOptions([
            { label: 'View Current Config', value: 'viewReportConfig', description: "See current report channels." },
            { label: 'Set Weekly Report Channel', value: 'weeklyReportChannel', description: 'Channel for Sunday activity reports.' },
            { label: 'Set Monthly Report Channel', value: 'monthlyReportChannel', description: 'Channel for 1st of month reports.' }
          ]);
      content = 'Configure **Analytics / Reports**:';
      break;

    default:
      console.warn(`[Interaction Handler] Unhandled system selection in handleSystemSelect: ${systemKey} for guild ${guild.id}`);
      if (interaction.isRepliable()) {
        await interaction.deferUpdate().catch(e => console.warn(`[Interaction Handler] Failed deferUpdate on unhandled system select ${systemKey}:`, e));
      }
      return;
  }

  if (menu) {
    const row = new ActionRowBuilder().addComponents(menu);
    await interaction.update({ content, components: [row] }).catch(e => console.error(`[Interaction Handler] Failed to update interaction for system select ${systemKey} in guild ${guild.id}:`, e));
  } else {
    console.error(`[Interaction Handler] No menu generated for valid system key: ${systemKey} in guild ${guild.id}`);
    await interaction.update({ content: "Error generating configuration options. Please try again.", components: [] }).catch(e => console.error(`[Interaction Handler] Failed to update interaction for menu generation error in guild ${guild.id}:`, e));
  }
}

async function handleStreakOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;

  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information. Cannot proceed.");
  if (!config) return handleInteractionError(interaction, "Configuration error occurred. Cannot proceed.");

  console.log(`[Config] User ${interaction.user.id} selected streak option '${option}' in guild ${guildId}`);

  switch (option) {
    case 'viewStreakConfig': {
      const streakSystem = config.streakSystem || {};
      const milestones = Object.entries(streakSystem)
          .filter(([key, value]) => key.startsWith('role') && key.endsWith('day') && value)
          .map(([key, value]) => {
            const days = key.replace('role', '').replace('day', '');
            const role = guild.roles.cache.get(value);
            const roleName = role ? `@${role.name}` : 'Unknown/Deleted Role';
            return `> ${days} Days: ${roleName} (${value})`;
          })
          .sort((a, b) => parseInt(a.split(' ')[1], 10) - parseInt(b.split(' ')[1], 10))
          .join('\n') || '> None set';
      const outputChannelId = streakSystem.channelStreakOutput;
      const outputChannelName = outputChannelId ? `<#${outputChannelId}> (${outputChannelId})` : 'Not Set (Uses current channel)';
      const enabledText = streakSystem.enabled ? '✅ Yes' : '❌ No';
      const threshold = streakSystem.streakThreshold ?? 'Default (10)';

      await interaction.reply({
        content: `**Streak System Config (Guild: ${guild.name})**\nEnabled: ${enabledText}\nThreshold: \`${threshold}\` messages/day\nOutput Channel: ${outputChannelName}\n\n**Milestones:**\n${milestones}`,
        ephemeral: true,
      }).catch(e => console.error(`[Interaction Handler] Failed to send viewStreakConfig reply for guild ${guildId}:`, e));
      break;
    }
    case 'toggleStreak':
      config.streakSystem.enabled = !config.streakSystem.enabled;
      try {
        await saveConfig(guildId, config);
        await interaction.update({ content: `✅ Streak System has been **${config.streakSystem.enabled ? 'enabled' : 'disabled'}**.`, components: [] });
      } catch (error) {
        console.error(`[Config] Failed to save/update toggleStreak for guild ${guildId}:`, error);
        await handleInteractionError(interaction, `Failed to ${config.streakSystem.enabled ? 'enable' : 'disable'} Streak System.`);
      }
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
      console.warn(`[Interaction Handler] Unknown streak option selected: ${option} in guild ${guildId}`);
      await interaction.reply({ content: 'Unknown or invalid streak option selected.', ephemeral: true }).catch(e => console.error(`[Interaction Handler] Failed to send unknown streak option reply for guild ${guildId}:`, e));
  }
}

async function handleLeaderOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;

  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information. Cannot proceed.");
  if (!config) return handleInteractionError(interaction, "Configuration error occurred. Cannot proceed.");

  console.log(`[Config] User ${interaction.user.id} selected leader option '${option}' in guild ${guildId}`);

  const leaderSystem = config.messageLeaderSystem || {};

  switch (option) {
    case 'viewLeaderConfig': {
      const channelId = leaderSystem.channelMessageLeader;
      const roleId = leaderSystem.roleMessageLeader;
      const channelName = channelId ? `<#${channelId}> (${channelId})` : 'Not Set';
      const role = roleId ? guild.roles.cache.get(roleId) : null;
      const roleName = role ? `@${role.name} (${roleId})` : 'Not Set';
      const enabledText = leaderSystem.enabled ? '✅ Yes' : '❌ No';

      await interaction.reply({
        content: `**Message Leader System Config (Guild: ${guild.name})**\nEnabled: ${enabledText}\nAnnouncement Channel: ${channelName}\nWinner Role: ${roleName}`,
        ephemeral: true,
      }).catch(e => console.error(`[Interaction Handler] Failed to send viewLeaderConfig reply for guild ${guildId}:`, e));
      break;
    }
    case 'toggleLeader':
      config.messageLeaderSystem.enabled = !leaderSystem.enabled;
      try {
        await saveConfig(guildId, config);
        await interaction.update({ content: `✅ Message Leader System has been **${config.messageLeaderSystem.enabled ? 'enabled' : 'disabled'}**.`, components: [] });
      } catch (error) {
        console.error(`[Config] Failed to save/update toggleLeader for guild ${guildId}:`, error);
        await handleInteractionError(interaction, `Failed to ${config.messageLeaderSystem.enabled ? 'enable' : 'disable'} Message Leader System.`);
      }
      break;
    case 'channelMessageLeader':
      await setChannel(interaction, config, guildId, 'messageLeaderSystem.channelMessageLeader', 'Message Leader Announcement Channel');
      break;
    case 'roleMessageLeader':
      await setRole(interaction, config, guildId, 'messageLeaderSystem.roleMessageLeader', 'Message Leader Winner Role');
      break;
    default:
      console.warn(`[Interaction Handler] Unknown leader option selected: ${option} in guild ${guildId}`);
      await interaction.reply({ content: 'Unknown or invalid leader option selected.', ephemeral: true }).catch(e => console.error(`[Interaction Handler] Failed to send unknown leader option reply for guild ${guildId}:`, e));
  }
}

async function handleLevelOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;

  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information. Cannot proceed.");
  if (!config) return handleInteractionError(interaction, "Configuration error occurred. Cannot proceed.");

  console.log(`[Config] User ${interaction.user.id} selected level option '${option}' in guild ${guildId}`);

  const levelSystem = config.levelSystem || {};

  switch(option) {
    case 'viewLevelConfig': {
      const milestones = Object.entries(levelSystem)
          .filter(([key, value]) => key.startsWith('roleLevel') && value)
          .map(([key, value]) => {
            const level = key.replace('roleLevel', '');
            const role = guild.roles.cache.get(value);
            const roleName = role ? `@${role.name}` : 'Unknown/Deleted Role';
            return `> Level ${level}: ${roleName} (${value})`;
          })
          .sort((a, b) => parseInt(a.split(' ')[1].replace('Level',''), 10) - parseInt(b.split(' ')[1].replace('Level',''), 10))
          .join('\n') || '> None set';
      const channelId = levelSystem.channelLevelUp;
      const channelName = channelId ? `<#${channelId}> (${channelId})` : 'Not Set (Uses current channel)';
      const enabledText = levelSystem.enabled ? '✅ Yes' : '❌ No';
      const pingsEnabledText = levelSystem.levelUpMessages !== false ? '✅ Yes' : '❌ No';
      const xpPerMsg = levelSystem.xpPerMessage ?? 'Default (10)';
      const multiplier = levelSystem.levelMultiplier ?? 'Default (1.5)';

      await interaction.reply({
        content: `**Level System Config (Guild: ${guild.name})**\nEnabled: ${enabledText}\nXP Per Message: \`${xpPerMsg}\`\nMultiplier: \`${multiplier}\`\nLevel Up Channel: ${channelName}\nLevel Up Pings: ${pingsEnabledText}\n\n**Milestones:**\n${milestones}`,
        ephemeral: true,
      }).catch(e => console.error(`[Interaction Handler] Failed to send viewLevelConfig reply for guild ${guildId}:`, e));
      break;
    }
    case 'toggleLevel':
      config.levelSystem.enabled = !levelSystem.enabled;
      try {
        await saveConfig(guildId, config);
        await interaction.update({ content: `✅ Level System has been **${config.levelSystem.enabled ? 'enabled' : 'disabled'}**.`, components: [] });
      } catch (error) {
        console.error(`[Config] Failed to save/update toggleLevel for guild ${guildId}:`, error);
        await handleInteractionError(interaction, `Failed to ${config.levelSystem.enabled ? 'enable' : 'disable'} Level System.`);
      }
      break;
    case 'channelLevelUp':
      await setChannel(interaction, config, guildId, 'levelSystem.channelLevelUp', 'Level-Up Message Channel');
      break;
    case 'xpPerMessage':
      await setNumericValue(interaction, config, guildId, 'levelSystem.xpPerMessage', 'XP per message', { min: 0, max: 1000, integer: true });
      break;
    case 'levelMultiplier':
      await setNumericValue(interaction, config, guildId, 'levelSystem.levelMultiplier', 'Level difficulty multiplier', { min: 1.01, max: 5.0, integer: false });
      break;
    case 'toggleLevelMsgs':
      config.levelSystem.levelUpMessages = !(levelSystem.levelUpMessages !== false);
      try {
        await saveConfig(guildId, config);
        await interaction.update({ content: `✅ Level Up Messages have been **${config.levelSystem.levelUpMessages ? 'enabled' : 'disabled'}**.`, components: [] });
      } catch (error) {
        console.error(`[Config] Failed to save/update toggleLevelMsgs for guild ${guildId}:`, error);
        await handleInteractionError(interaction, `Failed to ${config.levelSystem.levelUpMessages ? 'enable' : 'disable'} Level Up Messages.`);
      }
      break;
    case 'addLevelMilestone':
      await addMilestone(interaction, config, guildId, 'level');
      break;
    case 'removeLevelMilestone':
      await removeMilestone(interaction, config, guildId, 'level');
      break;
    default:
      console.warn(`[Interaction Handler] Unknown level option selected: ${option} in guild ${guildId}`);
      await interaction.reply({ content: 'Unknown or invalid level option selected.', ephemeral: true }).catch(e => console.error(`[Interaction Handler] Failed to send unknown level option reply for guild ${guildId}:`, e));
  }
}

async function handleReportOptions(interaction, config) {
  const option = interaction.values[0];
  const { guild } = interaction;
  const guildId = guild.id;

  if (!guild) return handleInteractionError(interaction, "Could not resolve guild information. Cannot proceed.");
  if (!config) return handleInteractionError(interaction, "Configuration error occurred. Cannot proceed.");

  console.log(`[Config] User ${interaction.user.id} selected report option '${option}' in guild ${guildId}`);

  const reportSettings = config.reportSettings || {};

  switch(option) {
    case 'viewReportConfig': {
      const weeklyChId = reportSettings.weeklyReportChannel;
      const monthlyChId = reportSettings.monthlyReportChannel;
      const weeklyCh = weeklyChId ? `<#${weeklyChId}> (${weeklyChId})` : 'Not Set';
      const monthlyCh = monthlyChId ? `<#${monthlyChId}> (${monthlyChId})` : 'Not Set';

      await interaction.reply({
        content: `**Analytics / Report Config (Guild: ${guild.name})**\nWeekly Report Channel: ${weeklyCh}\nMonthly Report Channel: ${monthlyCh}`,
        ephemeral: true,
      }).catch(e => console.error(`[Interaction Handler] Failed to send viewReportConfig reply for guild ${guildId}:`, e));
      break;
    }
    case 'weeklyReportChannel':
      await setChannel(interaction, config, guildId, 'reportSettings.weeklyReportChannel', 'Weekly Report Channel');
      break;
    case 'monthlyReportChannel':
      await setChannel(interaction, config, guildId, 'reportSettings.monthlyReportChannel', 'Monthly Report Channel');
      break;
    default:
      console.warn(`[Interaction Handler] Unknown report option selected: ${option} in guild ${guildId}`);
      await interaction.reply({ content: 'Unknown or invalid report option selected.', ephemeral: true }).catch(e => console.error(`[Interaction Handler] Failed to send unknown report option reply for guild ${guildId}:`, e));
  }
}

async function setChannel(interaction, config, guildId, configKey, description) {
  console.log(`[Config] Prompting user ${interaction.user.id} for channel: ${description} (Key: ${configKey}) in guild ${guildId}`);
  try {
    await interaction.deferUpdate();
  } catch (deferError) {
    console.warn(`[Config] Failed to defer update for setChannel prompt (${configKey}) in guild ${guildId}:`, deferError);

  }
  const promptMessage = await interaction.followUp({
    content: `Mention the text channel you want to use for **${description}**, or type \`clear\` to unset it.`,
    ephemeral: true, fetchReply: true
  }).catch(e => { console.error(`[Config] Failed to send setChannel prompt for ${configKey} in guild ${guildId}:`, e); return null; });

  if (!promptMessage) return;

  const filter = (msg) => msg.author.id === interaction.user.id && msg.channelId === interaction.channelId && msg.guildId === guildId;
  const collector = interaction.channel.createMessageCollector({ filter, time: 45000, max: 1 });

  collector.on('collect', async (msg) => {
    let feedbackMsg = '';
    let success = false;
    try {
      if (msg.content.toLowerCase() === 'clear') {
        set(config, configKey, null);
        feedbackMsg = `✅ **${description}** channel has been cleared.`;
        success = true;
      } else {
        const mentionedChannel = msg.mentions.channels.first();
        if (mentionedChannel && mentionedChannel.type === ChannelType.GuildText) {
          if (!mentionedChannel.permissionsFor(interaction.guild.members.me)?.has(PermissionsBitField.Flags.SendMessages)) {
            feedbackMsg = `❌ I don't have permission to send messages in ${mentionedChannel}. Please check my permissions.`;
            success = false;
          } else {
            set(config, configKey, mentionedChannel.id);
            feedbackMsg = `✅ **${description}** set to ${mentionedChannel}.`;
            success = true;
          }
        } else {
          feedbackMsg = '❌ Invalid input. Please mention a valid text channel or type `clear`.';
          success = false;
        }
      }

      if (success) {
        try {
          await saveConfig(guildId, config);
          console.log(`[Config] Successfully set channel ${configKey} to ${config[configKey] || 'null'} for guild ${guildId}`);
        } catch (saveError) {
          console.error(`[Config] Failed to save config after setting channel ${configKey} for guild ${guildId}:`, saveError);
          feedbackMsg = `❌ Failed to save configuration. Please try again.`;
          success = false;
        }
      }

      await interaction.followUp({ content: feedbackMsg, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send feedback for setChannel ${configKey} in guild ${guildId}:`, e));
      await msg.delete().catch(console.warn);
    } catch (error) {
      console.error(`[Config] Error in setChannel message collector for ${configKey} in guild ${guildId}:`, error);
      await handleInteractionError(interaction, 'An error occurred while processing the channel selection.');
    } finally {
      collector.stop('collected');
    }
  });

  collector.on('end', (collected, reason) => {
    if (reason === 'time') {
      interaction.followUp({ content: `⏰ Configuration for **${description}** cancelled (timed out).`, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send timeout message for setChannel ${configKey} in guild ${guildId}:`, e));
      console.log(`[Config] setChannel collector timed out for ${configKey} in guild ${guildId}`);
    }
  });
}

async function setNumericValue(interaction, config, guildId, configKey, description, options = {}) {
  const { min = 0, max = Infinity, integer = false } = options;
  console.log(`[Config] Prompting user ${interaction.user.id} for numeric value: ${description} (Key: ${configKey}, Min: ${min}, Max: ${max}, Int: ${integer}) in guild ${guildId}`);
  try {
    await interaction.deferUpdate();
  } catch (deferError) {
    console.warn(`[Config] Failed to defer update for setNumericValue prompt (${configKey}) in guild ${guildId}:`, deferError);

  }
  const promptMessage = await interaction.followUp({
    content: `Enter the value for **${description}**.\n(Allowed range: ${min} - ${max === Infinity ? 'No Limit' : max}${integer ? ', Whole numbers only' : ''})`,
    ephemeral: true, fetchReply: true
  }).catch(e => { console.error(`[Config] Failed to send setNumericValue prompt for ${configKey} in guild ${guildId}:`, e); return null; });

  if (!promptMessage) return;

  const filter = (msg) => msg.author.id === interaction.user.id && msg.channelId === interaction.channelId && msg.guildId === guildId;
  const collector = interaction.channel.createMessageCollector({ filter, time: 45000, max: 1 });

  collector.on('collect', async (msg) => {
    let feedbackMsg = '';
    let success = false;
    try {
      const rawInput = msg.content;
      const value = integer ? parseInt(rawInput, 10) : parseFloat(rawInput);

      if (isNaN(value)) {
        feedbackMsg = `❌ Invalid input. Please enter a number.`;
      } else if (value < min) {
        feedbackMsg = `❌ Value must be ${min} or higher.`;
      } else if (value > max) {
        feedbackMsg = `❌ Value must be ${max === Infinity ? 'lower' : max + ' or lower'}.`;
      } else if (integer && !Number.isInteger(value)) {
        feedbackMsg = `❌ Please enter a whole number (no decimals).`;
      } else {
        set(config, configKey, value);
        feedbackMsg = `✅ **${description}** set to \`${value}\`.`;
        success = true;
      }

      if (success) {
        try {
          await saveConfig(guildId, config);
          console.log(`[Config] Successfully set numeric value ${configKey} to ${value} for guild ${guildId}`);
        } catch (saveError) {
          console.error(`[Config] Failed to save config after setting numeric value ${configKey} for guild ${guildId}:`, saveError);
          feedbackMsg = `❌ Failed to save configuration. Please try again.`;
          success = false;
        }
      }
      await interaction.followUp({ content: feedbackMsg, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send feedback for setNumericValue ${configKey} in guild ${guildId}:`, e));
      await msg.delete().catch(console.warn);
    } catch (error) {
      console.error(`[Config] Error in setNumericValue message collector for ${configKey} in guild ${guildId}:`, error);
      await handleInteractionError(interaction, 'An error occurred while processing the numeric input.');
    } finally {
      collector.stop('collected');
    }
  });

  collector.on('end', (collected, reason) => {
    if (reason === 'time') {
      interaction.followUp({ content: `⏰ Configuration for **${description}** cancelled (timed out).`, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send timeout message for setNumericValue ${configKey} in guild ${guildId}:`, e));
      console.log(`[Config] setNumericValue collector timed out for ${configKey} in guild ${guildId}`);
    }
  });
}

async function setRole(interaction, config, guildId, configKey, description) {
  console.log(`[Config] Prompting user ${interaction.user.id} for role: ${description} (Key: ${configKey}) in guild ${guildId}`);
  try {
    await interaction.deferUpdate();
  } catch (deferError) {
    console.warn(`[Config] Failed to defer update for setRole prompt (${configKey}) in guild ${guildId}:`, deferError);

  }
  const promptMessage = await interaction.followUp({
    content: `Mention the role you want to use for **${description}**, or type \`clear\` to unset it.`,
    ephemeral: true, fetchReply: true
  }).catch(e => { console.error(`[Config] Failed to send setRole prompt for ${configKey} in guild ${guildId}:`, e); return null; });

  if (!promptMessage) return;

  const filter = (msg) => msg.author.id === interaction.user.id && msg.channelId === interaction.channelId && msg.guildId === guildId;
  const collector = interaction.channel.createMessageCollector({ filter, time: 45000, max: 1 });

  collector.on('collect', async (msg) => {
    let feedbackMsg = '';
    let success = false;
    try {
      if (msg.content.toLowerCase() === 'clear') {
        set(config, configKey, null);
        feedbackMsg = `✅ **${description}** role cleared.`;
        success = true;
      } else {
        const mentionedRole = msg.mentions.roles.first();
        if (!mentionedRole) {
          feedbackMsg = '❌ Invalid input. Please mention a role or type `clear`.';
        } else if (mentionedRole.id === guildId) {
          feedbackMsg = '❌ The @everyone role cannot be used.';
        } else if (mentionedRole.managed) {
          feedbackMsg = `❌ Cannot use managed roles like ${mentionedRole} (e.g., bot roles, integration roles).`;
        } else if (interaction.guild.members.me?.roles.highest.position <= mentionedRole.position) {
          feedbackMsg = `❌ I cannot manage the role ${mentionedRole}. My highest role needs to be above it in the server's role list.`;
        } else {
          set(config, configKey, mentionedRole.id);
          feedbackMsg = `✅ **${description}** set to ${mentionedRole}.`;
          success = true;
        }
      }

      if (success) {
        try {
          await saveConfig(guildId, config);
          console.log(`[Config] Successfully set role ${configKey} to ${config[configKey] || 'null'} for guild ${guildId}`);
        } catch (saveError) {
          console.error(`[Config] Failed to save config after setting role ${configKey} for guild ${guildId}:`, saveError);
          feedbackMsg = `❌ Failed to save configuration. Please try again.`;
          success = false;
        }
      }

      await interaction.followUp({ content: feedbackMsg, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send feedback for setRole ${configKey} in guild ${guildId}:`, e));
      await msg.delete().catch(console.warn);
    } catch (error) {
      console.error(`[Config] Error in setRole message collector for ${configKey} in guild ${guildId}:`, error);
      await handleInteractionError(interaction, 'An error occurred while processing the role selection.');
    } finally {
      collector.stop('collected');
    }
  });

  collector.on('end', (collected, reason) => {
    if (reason === 'time') {
      interaction.followUp({ content: `⏰ Configuration for **${description}** cancelled (timed out).`, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send timeout message for setRole ${configKey} in guild ${guildId}:`, e));
      console.log(`[Config] setRole collector timed out for ${configKey} in guild ${guildId}`);
    }
  });
}

async function addMilestone(interaction, config, guildId, systemType) {
  console.log(`[Config] User ${interaction.user.id} starting addMilestone process for ${systemType} in guild ${guildId}`);
  const configPath = systemType === 'streak' ? 'streakSystem' : 'levelSystem';
  const numberPromptText = `Enter the number for the **${systemType} milestone** (e.g., for a 10-day streak, enter \`10\`; for Level 5, enter \`5\`):`;
  const rolePromptText = (roleNamePrefix) => `Now, mention the role to assign for **${roleNamePrefix}**, type \`create\` to make a new role named "${roleNamePrefix}", or type \`cancel\`.`;
  const numberLimits = systemType === 'streak' ? { min: 1, max: 3650, integer: true } : { min: 1, max: 100, integer: true };

  try {
    await interaction.deferUpdate();
  } catch (deferError) {
    console.warn(`[Config] Failed to defer update for addMilestone prompt (number) in guild ${guildId}:`, deferError);

  }

  const numberPromptMessage = await interaction.followUp({ content: numberPromptText, ephemeral: true, fetchReply: true })
      .catch(e => { console.error(`[Config] Failed to send addMilestone number prompt for ${systemType} in guild ${guildId}:`, e); return null; });
  if (!numberPromptMessage) return;

  const filterNumber = msg => msg.author.id === interaction.user.id && msg.channelId === interaction.channelId && msg.guildId === guildId;
  const numberCollector = interaction.channel.createMessageCollector({ filter: filterNumber, time: 45000, max: 1 });

  numberCollector.on('collect', async numberMsg => {
    let milestoneNumber;
    let roleNamePrefix;
    let configRoleKey;
    let numFeedbackMsg = '';
    let numSuccess = false;

    try {
      await numberMsg.delete().catch(console.warn);
      const rawNumberInput = numberMsg.content;
      milestoneNumber = safeParseNumber(rawNumberInput, NaN);

      if (isNaN(milestoneNumber)) {
        numFeedbackMsg = `❌ Invalid input. Please enter a number.`;
      } else if (milestoneNumber < numberLimits.min) {
        numFeedbackMsg = `❌ Milestone number must be ${numberLimits.min} or higher.`;
      } else if (milestoneNumber > numberLimits.max) {
        numFeedbackMsg = `❌ Milestone number must be ${numberLimits.max} or lower.`;
      } else if (numberLimits.integer && !Number.isInteger(milestoneNumber)) {
        numFeedbackMsg = `❌ Please enter a whole number (no decimals).`;
      } else {
        roleNamePrefix = systemType === 'streak' ? `${milestoneNumber}-Day Streak` : `Level ${milestoneNumber}`;
        configRoleKey = systemType === 'streak' ? `role${milestoneNumber}day` : `roleLevel${milestoneNumber}`;

        if (config[configPath]?.[configRoleKey]) {
          numFeedbackMsg = `⚠️ A milestone for ${systemType} **${milestoneNumber}** already exists. Please remove it first if you want to change the role.`;
        } else {
          numFeedbackMsg = `✅ Milestone number ${milestoneNumber} accepted.`;
          numSuccess = true;
        }
      }

      if (!numSuccess) {
        await interaction.followUp({ content: numFeedbackMsg, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send feedback for addMilestone number input in guild ${guildId}:`, e));
        return numberCollector.stop('invalid_number');
      }


      const rolePromptMessage = await interaction.followUp({ content: rolePromptText(roleNamePrefix), ephemeral: true, fetchReply: true })
          .catch(e => { console.error(`[Config] Failed to send addMilestone role prompt for ${systemType} ${milestoneNumber} in guild ${guildId}:`, e); return null; });
      if (!rolePromptMessage) return numberCollector.stop('failed_role_prompt');


      const filterRole = msg => msg.author.id === interaction.user.id && msg.channelId === interaction.channelId && msg.guildId === guildId;
      const roleCollector = interaction.channel.createMessageCollector({ filter: filterRole, time: 60000, max: 1 });

      roleCollector.on('collect', async roleMsg => {
        let targetRole = null;
        let roleFeedbackMsg = '';
        let roleSuccess = false;
        const roleContentLower = roleMsg.content.toLowerCase();

        try {
          await roleMsg.delete().catch(console.warn);

          if (roleContentLower === 'cancel') {
            roleFeedbackMsg = '❌ Milestone creation cancelled.';
          } else if (roleContentLower === 'create') {
            console.log(`[Config] Attempting to create role "${roleNamePrefix}" for ${systemType} milestone ${milestoneNumber} in guild ${guildId}`);
            if (!interaction.guild.members.me?.permissions.has(PermissionsBitField.Flags.ManageRoles)) {
              roleFeedbackMsg = `❌ I don't have the "Manage Roles" permission to create a new role.`;
            } else {
              try {
                targetRole = await interaction.guild.roles.create({ name: roleNamePrefix, reason: `${systemType} milestone role for ${milestoneNumber}` });
                roleFeedbackMsg = `✅ Successfully created role ${targetRole} and set it for the milestone.`;
                roleSuccess = true;
              } catch (createError) {
                console.error(`[Config] Failed to create role "${roleNamePrefix}" in guild ${guildId}:`, createError);
                roleFeedbackMsg = `❌ Failed to create the role. Error: ${createError.message}. Make sure I have permissions.`;
              }
            }
          } else {
            targetRole = roleMsg.mentions.roles.first();
            if (!targetRole) {
              roleFeedbackMsg = '❌ No valid role mentioned. Please mention a role, or type `create` or `cancel`.';
            } else if (targetRole.id === guildId) {
              roleFeedbackMsg = '❌ The @everyone role cannot be used.';
              targetRole = null;
            } else if (targetRole.managed) {
              roleFeedbackMsg = `❌ Cannot use managed roles like ${targetRole}.`;
              targetRole = null;
            } else if (interaction.guild.members.me?.roles.highest.position <= targetRole.position) {
              roleFeedbackMsg = `❌ I cannot manage the role ${targetRole}. My highest role needs to be above it.`;
              targetRole = null;
            } else {
              roleFeedbackMsg = `✅ Role ${targetRole} selected for the milestone.`;
              roleSuccess = true;
            }
          }

          if (roleSuccess && targetRole) {
            set(config, `${configPath}.${configRoleKey}`, targetRole.id);
            try {
              await saveConfig(guildId, config);
              console.log(`[Config] Successfully added ${systemType} milestone ${milestoneNumber} with role ${targetRole.id} for guild ${guildId}`);
            } catch (saveError) {
              console.error(`[Config] Failed to save config after adding milestone role ${configRoleKey} for guild ${guildId}:`, saveError);
              roleFeedbackMsg = `❌ Failed to save configuration after setting the role. Please try again.`;
              roleSuccess = false;
            }
          }

          await interaction.followUp({ content: roleFeedbackMsg, ephemeral: true }).catch(e => console.warn(`[Config] Failed to send feedback for addMilestone role selection in guild ${guildId}:`, e));

        } catch (roleError) {
          console.error(`[Config] Error in addMilestone role message collector for ${systemType} ${milestoneNumber} in guild ${guildId}:`, roleError);
          await handleInteractionError(interaction, 'An error occurred while processing the role selection.');
        } finally {
          roleCollector.stop('collected_role');
        }
      });

      roleCollector.on('end', (collectedRole, roleReason) => {
        if (roleReason === 'time') {
          interaction.followUp({ content: '⏰ Role selection cancelled (timed out).', ephemeral: true }).catch(e => console.warn(`[Config] Failed to send timeout message for addMilestone role selection in guild ${guildId}:`, e));
          console.log(`[Config] addMilestone role collector timed out for ${systemType} ${milestoneNumber} in guild ${guildId}`);
        }
      });

    } catch (numberError) {
      console.error(`[Config] Error in addMilestone number message collector for ${systemType} in guild ${guildId}:`, numberError);
      await handleInteractionError(interaction, 'An error occurred while processing the milestone number.');
    } finally {
      numberCollector.stop('collected_number');
    }
  });

  numberCollector.on('end', (collectedNumber, numberReason) => {
    if (numberReason === 'time') {
      interaction.followUp({ content: '⏰ Milestone number input cancelled (timed out).', ephemeral: true }).catch(e => console.warn(`[Config] Failed to send timeout message for addMilestone number input in guild ${guildId}:`, e));
      console.log(`[Config] addMilestone number collector timed out for ${systemType} in guild ${guildId}`);
    }
  });
}


async function removeMilestone(interaction, config, guildId, systemType) {
  console.log(`[Config] User ${interaction.user.id} starting removeMilestone process for ${systemType} in guild ${guildId}`);
  try {
    await interaction.deferUpdate();
  } catch (deferError) {
    console.warn(`[Config] Failed to defer update for removeMilestone prompt (${systemType}) in guild ${guildId}:`, deferError);

  }

  const configPath = systemType === 'streak' ? 'streakSystem' : 'levelSystem';
  const milestonePrefix = systemType === 'streak' ? 'role' : 'roleLevel';
  const milestoneSuffix = systemType === 'streak' ? 'day' : '';

  const milestones = Object.entries(config[configPath] ?? {})
      .filter(([key]) => key.startsWith(milestonePrefix) && (systemType === 'level' || key.endsWith(milestoneSuffix)))
      .map(([key, roleId]) => {
        const numberMatch = key.replace(milestonePrefix, '').replace(milestoneSuffix, '');
        const number = parseInt(numberMatch, 10);
        if (isNaN(number) || !roleId) return null;
        const role = interaction.guild.roles.cache.get(roleId);
        const roleName = role ? role.name : 'Deleted Role';
        const label = systemType === 'streak' ? `${number} Days` : `Level ${number}`;
        return { number, roleId, roleName, key, label };
      })
      .filter(m => m !== null)
      .sort((a, b) => a.number - b.number);

  if (milestones.length === 0) {
    console.log(`[Config] No ${systemType} milestones found to remove in guild ${guildId}.`);
    return interaction.followUp({ content: `✅ There are currently no configured **${systemType}** milestones to remove.`, ephemeral: true })
        .catch(e => console.warn(`[Config] Failed to send no milestones message for ${systemType} remove in guild ${guildId}:`, e));
  }

  const options = milestones.map(m => ({
    label: m.label,
    description: `Role: @${m.roleName} (ID: ${m.roleId})`,
    value: m.key
  }));

  const selectMenuId = `remove_${systemType}_milestone_select_${interaction.id}`;
  const selectMenu = new StringSelectMenuBuilder()
      .setCustomId(selectMenuId)
      .setPlaceholder(`Select ${systemType} milestone to remove`)
      .addOptions(options.slice(0, 25));

  const row = new ActionRowBuilder().addComponents(selectMenu);

  const removalPrompt = await interaction.followUp({
    content: `Select the **${systemType} milestone** you wish to remove from the configuration (this does **not** delete the role itself):`,
    components: [row],
    ephemeral: true,
    fetchReply: true
  }).catch(e => { console.error(`[Config] Failed to send removeMilestone prompt for ${systemType} in guild ${guildId}:`, e); return null; });

  if (!removalPrompt) return;

  const filter = i => i.customId === selectMenuId && i.user.id === interaction.user.id;
  const collector = removalPrompt.createMessageComponentCollector({ filter, time: 45000, max: 1 });

  collector.on('collect', async i => {
    let feedbackMsg = '';
    let success = false;
    try {
      await i.deferUpdate().catch(e => console.warn(`[Config] Failed deferUpdate on remove milestone selection for ${systemType} in guild ${guildId}:`, e));
      const selectedKey = i.values[0];
      const milestoneToRemove = milestones.find(m => m.key === selectedKey);

      if (!milestoneToRemove) {
        feedbackMsg = '❌ Invalid or expired selection. Please try removing again.';
      } else if (config[configPath]?.[selectedKey]) {
        delete config[configPath][selectedKey];
        try {
          await saveConfig(guildId, config);
          feedbackMsg = `✅ Removed **${milestoneToRemove.label}** milestone (was Role: @${milestoneToRemove.roleName}) from the configuration.`;
          success = true;
          console.log(`[Config] Successfully removed ${systemType} milestone ${selectedKey} for guild ${guildId}`);
        } catch (saveError) {
          console.error(`[Config] Failed to save config after removing milestone ${selectedKey} for guild ${guildId}:`, saveError);
          feedbackMsg = `❌ Failed to save configuration after removing the milestone. Please try again.`;

          config[configPath][selectedKey] = milestoneToRemove.roleId;
        }
      } else {
        feedbackMsg = `❓ Milestone configuration for key "${selectedKey}" was not found. It might have been removed already.`;
      }
      await interaction.editReply({ content: feedbackMsg, components: [] }).catch(e => console.warn(`[Config] Failed to edit reply after remove milestone selection for ${systemType} in guild ${guildId}:`, e));
    } catch (error) {
      console.error(`[Config] Error processing remove milestone selection ${i.values[0]} for ${systemType} in guild ${guildId}:`, error);
      await handleInteractionError(interaction, 'An error occurred while removing the milestone.');
    } finally {
      collector.stop('collected');
    }
  });

  collector.on('end', (collected, reason) => {
    if (reason === 'time') {
      interaction.editReply({ content: '⏰ Milestone removal cancelled (timed out).', components: [] }).catch(e => console.warn(`[Config] Failed to edit reply for remove milestone timeout (${systemType}) in guild ${guildId}:`, e));
      console.log(`[Config] removeMilestone collector timed out for ${systemType} in guild ${guildId}`);
    }
  });
}