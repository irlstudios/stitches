const { Collection, StringSelectMenuBuilder, ActionRowBuilder } = require('discord.js');
const { set } = require('lodash');
const { getConfig, saveConfig } = require('./configManager');

const cooldowns = new Collection();

module.exports = async (client, interaction) => {
  try {
    if (interaction.isCommand()) {
      const command = client.commands.get(interaction.commandName);
      if (!command) {
        return interaction.reply({
          content: 'This command is no longer available.',
          ephemeral: true,
        });
      }

      const now = Date.now();
      const cooldownAmount = 3000;
      const timestamps = cooldowns.get(interaction.user.id);

      if (timestamps) {
        const expirationTime = timestamps + cooldownAmount;
        if (now < expirationTime) {
          const timeLeft = (expirationTime - now) / 1000;
          return interaction.reply({
            content: `Please wait ${timeLeft.toFixed(1)} more second(s) before reusing the \`${interaction.commandName}\` command.`,
            ephemeral: true,
          });
        }
      }

      cooldowns.set(interaction.user.id, now);
      setTimeout(() => cooldowns.delete(interaction.user.id), cooldownAmount);

      try {
        await command.execute(interaction);
      } catch (error) {
        console.error(`Error executing command ${interaction.commandName}:`, error);
        handleInteractionError(interaction, 'There was an error while executing this command!');
      }
    } else if (interaction.isStringSelectMenu()) {
      const { guild, customId } = interaction;
      if (!guild) {
        return interaction.reply({
          content: "This action is only available within a server (guild).",
          ephemeral: true,
        });
      }

      let config;
      try {
        config = await getConfig(guild.id);
        if (!config) {
          return interaction.reply({
            content: 'Configuration for this guild is missing.',
            ephemeral: true,
          });
        }
      } catch (error) {
        console.error("Error loading configuration:", error);
        return interaction.reply({
          content: 'An error occurred while loading the configuration.',
          ephemeral: true,
        });
      }

      ensureConfigStructure(config);

      try {
        if (customId === 'system-select') {
          await handleSystemSelect(interaction, config, guild);
        } else if (customId === 'streak-options') {
          await handleStreakOptions(interaction, config, guild);
        } else if (customId === 'leader-options') {
          await handleLeaderOptions(interaction, config, guild);
        } else if (customId === 'level-options') {
          await handleLevelOptions(interaction, config, guild);
        } else if (customId === 'weeklyReportSystem') {
          await handleReportOptions(interaction, config, guild);
        }
      } catch (error) {
        console.error(`Error handling select menu interaction (${customId}):`, error);
        handleInteractionError(interaction, 'There was an error processing your selection.');
      }
    }
  } catch (error) {
    console.error('Critical error handling interaction:', error);
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: 'A critical error occurred. Please try again later.',
        ephemeral: true,
      });
    }
  }
};

async function handleInteractionError(interaction, errorMessage) {
  try {
    if (!interaction.replied && !interaction.deferred) {
      await interaction.reply({
        content: errorMessage,
        ephemeral: true,
      });
    } else if (interaction.deferred) {
      await interaction.editReply({
        content: errorMessage,
      });
    }
  } catch (replyError) {
    console.error('Failed to send error reply:', replyError);
  }
}

// Updated to include new attributes for expiration and lastUpdated in config objects
function ensureConfigStructure(config) {
  if (!config.streakSystem) {
    config.streakSystem = {
      enabled: false,
      streakThreshold: 10,
    };
  }
  if (!config.messageLeaderSystem) {
    config.messageLeaderSystem = {
      enabled: false,
    };
  }
  if (!config.levelSystem) {
    config.levelSystem = {
      enabled: false,
      xpPerMessage: 10,
      levelMultiplier: 1.5,
      rewards: {},
    };
  }
  if (!config.reportSettings) {
    config.reportSettings = {
      weeklyReportChannel: "",
      monthlyReportChannel: ""
    };
  }
  // Optional: Set default metadata if not present
  if (!config.lastUpdated) {
    config.lastUpdated = new Date().toISOString();
  }
  if (!config.expireAt) {
    config.expireAt = null;
  }
}

async function handleSystemSelect(interaction, config, guild) {
  const system = interaction.values[0];
  let menu, content;

  if (system === 'streakSystem') {
    menu = new StringSelectMenuBuilder()
        .setCustomId('streak-options')
        .setPlaceholder('Select a streak system option')
        .addOptions([
          { label: 'View Config', value: 'viewStreakConfig' },
          { label: 'Add Milestone', value: 'addMilestone' },
          { label: 'Remove Milestone', value: 'removeMilestone' },
          { label: 'Streak Output Channel', value: 'channelStreakOutput' },
          { label: 'Streak Threshold', value: 'streakThreshold' },
          { label: 'Enable Streak System', value: 'enableStreak' },
          { label: 'Disable Streak System', value: 'disableStreak' },
        ]);
    content = 'Configure the Streak System:';
  } else if (system === 'messageLeaderSystem') {
    menu = new StringSelectMenuBuilder()
        .setCustomId('leader-options')
        .setPlaceholder('Select a message leader system option')
        .addOptions([
          { label: 'View Config', value: 'viewLeaderConfig' },
          { label: 'Message Leader Announcement Channel', value: 'channelMessageLeader' },
          { label: 'Message Leader Winner Role', value: 'roleMessageLeader' },
          { label: 'Enable Message Leader System', value: 'enableLeader' },
          { label: 'Disable Message Leader System', value: 'disableLeader' },
        ]);
    content = 'Configure the Message Leader System:';
  } else if (system === 'levelSystem') {
    menu = new StringSelectMenuBuilder()
        .setCustomId('level-options')
        .setPlaceholder('Select a level system option')
        .addOptions([
          { label: 'View Config', value: 'viewLevelConfig' },
          { label: 'XP per Message', value: 'xpPerMessage' },
          { label: 'XP Increment', value: 'levelMultiplier' },
          { label: 'Level-Up Message Channel', value: 'channelLevelUp' },
          { label: 'Enable Level System', value: 'enableLevel' },
          { label: 'Disable Level System', value: 'disableLevel' },
          { label: 'Add Milestone', value: 'addLevelMilestone' },
          { label: 'Remove Milestone', value: 'removeLevelMilestone' },
        ]);
    content = 'Configure the Level System:';
  } else if (system === 'weeklyReportSystem') {
    menu = new StringSelectMenuBuilder()
        .setCustomId('report-options')
        .setPlaceholder('Select a report system option')
        .addOptions([
          { label: 'View Config', value: 'viewReportConfig' },
          { label: 'Weekly Report Channel', value: 'weeklyReportChannel' },
          { label: 'Monthly Report Channel', value: 'monthlyReportChannel' }
        ]);
    content = 'Configure the Analytics System:';
  }

  const row = new ActionRowBuilder().addComponents(menu);
  await interaction.update({ content, components: [row] });
}

async function handleStreakOptions(interaction, config, guild) {
  const option = interaction.values[0];

  if (option === 'viewStreakConfig') {
    await interaction.reply({
      content: `Current Streak System Config:\nEnabled: ${config.streakSystem.enabled}\nThreshold: ${config.streakSystem.streakThreshold}\nMilestones: ${
          Object.keys(config.streakSystem)
              .filter(key => key.startsWith('role') && key.endsWith('day'))
              .map(key => `${key.replace('role', '').replace('day', '')} days`)
              .join(', ')
      }`,
      ephemeral: true,
    });
  } else if (option === 'addMilestone') {
    await addMilestone(interaction, guild, config, guild.id, 'streak');
  } else if (option === 'removeMilestone') {
    await removeMilestone(interaction, guild, config, guild.id, 'streak');
  } else if (option === 'enableStreak' || option === 'disableStreak') {
    config.streakSystem.enabled = option === 'enableStreak';
    await saveConfig(guild.id, config);
    await interaction.update({ content: `Streak System has been ${option === 'enableStreak' ? 'enabled' : 'disabled'}.`, components: [] });
  } else if (option === 'channelStreakOutput') {
    await setChannel(interaction, guild, config, guild.id, 'streakSystem.channelStreakOutput', 'Streak Output Channel');
  } else if (option === 'streakThreshold') {
    await setThreshold(interaction, config, guild.id, 'streakSystem.streakThreshold', 'Streak threshold');
  }
}

async function handleLeaderOptions(interaction, config, guild) {
  const option = interaction.values[0];

  if (option === 'viewLeaderConfig') {
    await interaction.reply({
      content: `Current Message Leader System Config:\nEnabled: ${config.messageLeaderSystem.enabled}\nAnnouncement Channel: <#${config.messageLeaderSystem.channelMessageLeader || 'Not Set'}>\nWinner Role: <@&${config.messageLeaderSystem.roleMessageLeader || 'Not Set'}>`,
      ephemeral: true,
    });
  } else if (option === 'enableLeader' || option === 'disableLeader') {
    config.messageLeaderSystem.enabled = option === 'enableLeader';
    await saveConfig(guild.id, config);
    await interaction.update({ content: `Message Leader System has been ${option === 'enableLeader' ? 'enabled' : 'disabled'}.`, components: [] });
  } else if (option === 'channelMessageLeader') {
    await setChannel(interaction, guild, config, guild.id, 'messageLeaderSystem.channelMessageLeader', 'Message Leader Announcement Channel');
  } else if (option === 'roleMessageLeader') {
    await setRole(interaction, guild, config, guild.id, 'messageLeaderSystem.roleMessageLeader', 'Message Leader Role');
  }
}

async function handleLevelOptions(interaction, config, guild) {
  const option = interaction.values[0];

  if (option === 'viewLevelConfig') {
    await interaction.reply({
      content: `Current Level System Config:\nEnabled: ${config.levelSystem.enabled}\nXP per Message: ${config.levelSystem.xpPerMessage}\nXP Increment: ${config.levelSystem.levelMultiplier}\nLevel-Up Message Channel: <#${config.levelSystem.channelLevelUp || 'Not Set'}>\nMilestones: ${
          Object.keys(config.levelSystem)
              .filter(key => key.startsWith('role') && key.includes('Level'))
              .map(key => `${key.replace('roleLevel', 'Level ')}`)
              .join(', ')
      }`,
      ephemeral: true,
    });
  } else if (option === 'enableLevel' || option === 'disableLevel') {
    config.levelSystem.enabled = option === 'enableLevel';
    await saveConfig(guild.id, config);
    await interaction.update({ content: `Level System has been ${option === 'enableLevel' ? 'enabled' : 'disabled'}.`, components: [] });
  } else if (option === 'xpPerMessage') {
    await setThreshold(interaction, config, guild.id, 'levelSystem.xpPerMessage', 'XP per message');
  } else if (option === 'levelMultiplier') {
    await setThreshold(interaction, config, guild.id, 'levelSystem.levelMultiplier', 'XP increment per level');
  } else if (option === 'channelLevelUp') {
    await setChannel(interaction, guild, config, guild.id, 'levelSystem.channelLevelUp', 'Level-Up Message Channel');
  } else if (option === 'addLevelMilestone') {
    await addMilestone(interaction, guild, config, guild.id, 'level');
  } else if (option === 'removeLevelMilestone') {
    await removeMilestone(interaction, guild, config, guild.id, 'level');
  }
}

async function handleReportOptions(interaction, config, guild) {
  const option = interaction.values[0];

  if (option === 'viewReportConfig') {
    await interaction.reply({
      content: `Current Report Settings:\nWeekly Report Channel: <#${config.reportSettings.weeklyReportChannel || 'Not Set'}>\nMonthly Report Channel: <#${config.reportSettings.monthlyReportChannel || 'Not Set'}>`,
      ephemeral: true,
    });
  } else if (option === 'weeklyReportChannel') {
    await setChannel(interaction, guild, config, guild.id, 'reportSettings.weeklyReportChannel', 'Weekly Report Channel');
  } else if (option === 'monthlyReportChannel') {
    await setChannel(interaction, guild, config, guild.id, 'reportSettings.monthlyReportChannel', 'Monthly Report Channel');
  }
}

async function setChannel(interaction, guild, config, guildId, configKey, description) {
  await interaction.deferReply({ ephemeral: true });
  await interaction.followUp({ content: `Please mention the channel for ${description} (e.g., #channel-name):` });

  const filter = (msg) =>
      msg.author.id === interaction.user.id && msg.guild.id === interaction.guild.id;

  const collector = interaction.channel.createMessageCollector({ filter, time: 15000, max: 1 });

  collector.on('collect', async (msg) => {
    const channel = msg.mentions.channels.first();
    await msg.delete();
    if (!channel || !channel.isTextBased()) {
      await interaction.followUp({ content: 'Please mention a valid text channel.', ephemeral: true });
    } else {
      set(config, configKey, channel.id);
      await saveConfig(guildId, config);
      await interaction.followUp({ content: `${description} has been set to ${channel.name}.`, ephemeral: true });
    }
  });

  collector.on('end', (collected) => {
    if (collected.size === 0) {
      interaction.followUp({ content: 'Time ran out. Please try the command again.', ephemeral: true });
    }
  });
}

async function setThreshold(interaction, config, guildId, configKey, description) {
  await interaction.deferReply({ ephemeral: true });
  await interaction.followUp({ content: `Please enter the ${description}:` });

  const filter = (msg) =>
      msg.author.id === interaction.user.id && msg.guild.id === interaction.guild.id;

  const collector = interaction.channel.createMessageCollector({ filter, time: 15000, max: 1 });

  collector.on('collect', async (msg) => {
    const value = parseFloat(msg.content);
    await msg.delete();
    if (isNaN(value) || value <= 0) {
      await interaction.followUp({ content: 'Please provide a valid number.', ephemeral: true });
    } else {
      set(config, configKey, value);
      await saveConfig(guildId, config);
      await interaction.followUp({ content: `${description} has been set to ${value}.`, ephemeral: true });
    }
  });

  collector.on('end', (collected) => {
    if (collected.size === 0) {
      interaction.followUp({ content: 'Time ran out. Please try the command again.', ephemeral: true });
    }
  });
}

async function setRole(interaction, guild, config, guildId, configKey, description) {
  await interaction.deferReply({ ephemeral: true });
  await interaction.followUp({ content: `Please mention the role for ${description} (e.g., @role-name):` });

  const filter = (msg) =>
      msg.author.id === interaction.user.id && msg.guild.id === interaction.guild.id;

  const collector = interaction.channel.createMessageCollector({ filter, time: 15000, max: 1 });

  collector.on('collect', async (msg) => {
    const role = msg.mentions.roles.first();
    await msg.delete();
    if (!role) {
      await interaction.followUp({ content: 'Please mention a valid role.', ephemeral: true });
    } else {
      set(config, configKey, role.id);
      await saveConfig(guildId, config);
      await interaction.followUp({ content: `${description} has been set to ${role.name}.`, ephemeral: true });
    }
  });

  collector.on('end', (collected) => {
    if (collected.size === 0) {
      interaction.followUp({ content: 'Time ran out. Please try the command again.', ephemeral: true });
    }
  });
}

async function addMilestone(interaction, guild, config, guildId, systemType) {
  await interaction.deferReply({ ephemeral: true });
  await interaction.followUp({ content: `Please enter the number of days/level for the milestone (e.g., 5 for 5-day streak or 5 for Level 5):` });

  const filter = (msg) =>
      msg.author.id === interaction.user.id && msg.guild.id === interaction.guild.id;

  const collector = interaction.channel.createMessageCollector({ filter, time: 15000, max: 1 });

  collector.on('collect', async (msg) => {
    const milestone = parseInt(msg.content, 10);
    await msg.delete();
    if (isNaN(milestone) || milestone <= 0) {
      await interaction.followUp({ content: 'Please provide a valid number.', ephemeral: true });
    } else {
      const roleName = systemType === 'streak' ? `${milestone} Day Streak` : `Level ${milestone}`;
      let milestoneRole = guild.roles.cache.find(role => role.name === roleName);
      if (!milestoneRole) {
        milestoneRole = await guild.roles.create({
          name: roleName,
          color: '#00FF00',
          reason: `Role for users with a ${milestone}-day streak or reaching Level ${milestone}`,
        });
      }
      if (systemType === 'streak') {
        config.streakSystem[`role${milestone}day`] = milestoneRole.id;
      } else {
        config.levelSystem[`roleLevel${milestone}`] = milestoneRole.id;
      }
      await saveConfig(guildId, config);
      await interaction.followUp({
        content: `Milestone for ${milestone} ${systemType === 'streak' ? 'days' : 'level'} has been added.`,
        ephemeral: true,
      });
    }
  });

  collector.on('end', (collected) => {
    if (collected.size === 0) {
      interaction.followUp({ content: 'Time ran out. Please try the command again.', ephemeral: true });
    }
  });
}

async function removeMilestone(interaction, guild, config, guildId, systemType) {
  await interaction.deferReply({ ephemeral: true });
  await interaction.followUp({ content: `Please enter the number of days/level for the milestone to remove:` });

  const filter = (msg) =>
      msg.author.id === interaction.user.id && msg.guild.id === interaction.guild.id;

  const collector = interaction.channel.createMessageCollector({ filter, time: 15000, max: 1 });

  collector.on('collect', async (msg) => {
    const milestone = parseInt(msg.content, 10);
    await msg.delete();
    if (isNaN(milestone) || milestone <= 0) {
      await interaction.followUp({ content: 'Please provide a valid number.', ephemeral: true });
    } else {
      const roleKey = systemType === 'streak' ? `role${milestone}day` : `roleLevel${milestone}`;
      const milestoneRoleId = systemType === 'streak'
          ? config.streakSystem[roleKey]
          : config.levelSystem[roleKey];
      if (!milestoneRoleId) {
        return interaction.followUp({ content: `No milestone for ${milestone} ${systemType === 'streak' ? 'days' : 'level'} found.`, ephemeral: true });
      }
      const milestoneRole = guild.roles.cache.get(milestoneRoleId);
      if (milestoneRole) {
        await milestoneRole.delete();
      }
      if (systemType === 'streak') {
        delete config.streakSystem[roleKey];
      } else {
        delete config.levelSystem[roleKey];
      }
      await saveConfig(guildId, config);
      await interaction.followUp({
        content: `Milestone for ${milestone} ${systemType === 'streak' ? 'days' : 'level'} has been removed.`,
        ephemeral: true,
      });
    }
  });

  collector.on('end', (collected) => {
    if (collected.size === 0) {
      interaction.followUp({ content: 'Time ran out. Please try the command again.', ephemeral: true });
    }
  });
}