const { SlashCommandBuilder, PermissionsBitField } = require('discord.js');
const { getConfig, saveConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
    .setName('setup-bot')
    .setDescription('Sets up the necessary channels, roles, and categories for the Streak Bot')
    .addStringOption(option =>
      option.setName('system')
        .setDescription('Select which system to set up')
        .setRequired(true)
        .addChoices(
          { name: 'Streak System', value: 'streak-system' },
          { name: 'Message Leader System', value: 'message-leader-system' },
          { name: 'Level System', value: 'level-system' },
          { name: 'All Systems', value: 'all-systems' }
        )
    ),
  async execute(interaction) {
    try {
      if (!interaction.member.permissions.has(PermissionsBitField.Flags.ManageGuild)) {
        return interaction.reply({
          content: 'You do not have permission to use this command. You need "Manage Server" permission to use it.',
          ephemeral: true
        });
      }
      await interaction.deferReply({ ephemeral: true });
      const guild = interaction.guild;
      const selectedSystem = interaction.options.getString('system');
      let config = await getConfig(guild.id) || {};
      ensureConfigStructure(config);

      let category = await getOrCreateCategory(guild, 'Engagement');

      if (selectedSystem === 'streak-system' || selectedSystem === 'all-systems') {
        await setupStreakSystem(guild, category, config);
      }
      if (selectedSystem === 'message-leader-system' || selectedSystem === 'all-systems') {
        await setupMessageLeaderSystem(guild, category, config);
      }
      if (selectedSystem === 'level-system' || selectedSystem === 'all-systems') {
        await setupLevelSystem(guild, category, config);
      }

      await saveConfig(guild.id, config);
      await interaction.editReply(`Bot setup completed successfully! The following system(s) have been configured: ${selectedSystem.replace('-', ' ')}`);
    } catch (error) {
      console.error(`Error executing command setup-bot: ${error.message}`);
      if (interaction.deferred) {
        await interaction.editReply({
          content: 'An error occurred during setup. Please try again later.'
        });
      }
    }
  },
};

async function getOrCreateCategory(guild, categoryName) {
  let category = guild.channels.cache.find(channel => channel.name === categoryName && channel.type === 4);
  if (!category) {
    category = await guild.channels.create({
      name: categoryName,
      type: 4,
    });
  }
  return category;
}

async function setChannelPermissions(channel) {
  await channel.permissionOverwrites.edit(channel.guild.id, {
    [PermissionsBitField.Flags.SendMessages]: false,
    [PermissionsBitField.Flags.ViewChannel]: true
  });
  await channel.permissionOverwrites.edit(channel.guild.client.user.id, {
    [PermissionsBitField.Flags.SendMessages]: true,
    [PermissionsBitField.Flags.ViewChannel]: true
  });
}

async function setupStreakSystem(guild, category, config) {
  if (config.streakSystem?.enabled) return;
  const streakOutputChannel = await getOrCreateChannel(guild, category, 'streak-up');
  const role2Day = await getOrCreateRole(guild, '2 Day Streak', 'Green');
  const role7Day = await getOrCreateRole(guild, '7 Day Streak', 'Gold');
  config.streakSystem = {
    enabled: true,
    channelStreakOutput: streakOutputChannel.id,
    role2day: role2Day.id,
    role7day: role7Day.id,
    streakThreshold: config.streakSystem?.streakThreshold || 4,
    isGymClassServer: config.streakSystem?.isGymClassServer || false,
  };
}

async function setupMessageLeaderSystem(guild, category, config) {
  if (config.messageLeaderSystem?.enabled) return;
  const messageLeaderChannel = await getOrCreateChannel(guild, category, 'message-leader-announcements');
  const messageLeaderRole = await getOrCreateRole(guild, 'Message Leader', 'Blue');
  config.messageLeaderSystem = {
    enabled: true,
    channelMessageLeader: messageLeaderChannel.id,
    roleMessageLeader: messageLeaderRole.id,
  };
}

async function setupLevelSystem(guild, category, config) {
  if (config.levelSystem?.enabled) return;
  const levelUpChannel = await getOrCreateChannel(guild, category, 'level-up');
  const level1Role = await getOrCreateRole(guild, 'Level 1', 'Blue');
  const level5Role = await getOrCreateRole(guild, 'Level 5', 'Purple');
  config.levelSystem = {
    enabled: true,
    channelLevelUp: levelUpChannel.id,
    roleLevel1: level1Role.id,
    roleLevel5: level5Role.id,
    xpPerMessage: config.levelSystem?.xpPerMessage || 10,
    levelMultiplier: config.levelSystem?.levelMultiplier || 1.5,
    levelUpMessages: config.levelSystem?.levelUpMessages || true,
  };
}

async function getOrCreateChannel(guild, category, channelName) {
  let channel = guild.channels.cache.find(c => c.name === channelName);
  if (!channel) {
    channel = await guild.channels.create({
      name: channelName,
      type: 0,
      parent: category.id,
    });
    await setChannelPermissions(channel);
  }
  return channel;
}

async function getOrCreateRole(guild, roleName, roleColor) {
  let role = guild.roles.cache.find(r => r.name === roleName);
  if (!role) {
    role = await guild.roles.create({
      name: roleName,
      color: roleColor,
      reason: `Role for users with ${roleName}`,
    });
  }
  return role;
}

function ensureConfigStructure(config) {
  config.streakSystem = config.streakSystem || {};
  config.messageLeaderSystem = config.messageLeaderSystem || {};
  config.levelSystem = config.levelSystem || {};
}