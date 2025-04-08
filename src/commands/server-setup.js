const { SlashCommandBuilder, PermissionsBitField, ActionRowBuilder, StringSelectMenuBuilder, ChannelType, Colors } = require('discord.js');
const { getConfig, saveConfig, ensureConfigStructure } = require('../configManager');

async function getOrCreateCategory(guild, categoryName) {
  let category = guild.channels.cache.find(channel =>
      channel.name.toLowerCase() === categoryName.toLowerCase() && channel.type === ChannelType.GuildCategory
  );
  if (!category) {
    console.log(`Creating category '${categoryName}' in guild ${guild.id}`);
    try {
      category = await guild.channels.create({
        name: categoryName,
        type: ChannelType.GuildCategory,
        reason: 'Category for bot engagement features'
      });
    } catch (error) {
      console.error(`Failed to create category '${categoryName}' in guild ${guild.id}:`, error);
      throw new Error(`Could not create category '${categoryName}'. Please check bot permissions (Manage Channels).`);
    }
  }
  return category;
}

async function setProtectedChannelPermissions(channel) {
  try {
    await channel.permissionOverwrites.edit(channel.guild.roles.everyone, {
      SendMessages: false,
      ViewChannel: true,
      AddReactions: false
    });
    await channel.permissionOverwrites.edit(channel.client.user.id, {
      SendMessages: true,
      ViewChannel: true,
      EmbedLinks: true,
      AttachFiles: true
    });
  } catch (error) {
    console.warn(`Could not set permissions for channel ${channel.name} (${channel.id}) in guild ${channel.guild.id}:`, error.message);
  }
}

async function getOrCreateProtectedChannel(guild, category, channelName) {
  let channel = guild.channels.cache.find(c =>
      c.name.toLowerCase() === channelName.toLowerCase() &&
      c.type === ChannelType.GuildText &&
      (category ? c.parentId === category.id : true)
  );

  if (!channel) {
    console.log(`Creating protected channel '#${channelName}' in guild ${guild.id}`);
    try {
      channel = await guild.channels.create({
        name: channelName,
        type: ChannelType.GuildText,
        parent: category?.id || null,
        topic: `Bot announcements for ${channelName.replace('-', ' ')}`,
        reason: `Channel for bot ${channelName.replace('-', ' ')} feature`
      });
      await setProtectedChannelPermissions(channel);
    } catch (error) {
      console.error(`Failed to create channel '#${channelName}' in guild ${guild.id}:`, error);
      throw new Error(`Could not create channel '#${channelName}'. Please check bot permissions (Manage Channels).`);
    }
  } else {
    await setProtectedChannelPermissions(channel);
  }
  return channel;
}

async function getOrCreateRole(guild, roleName, roleColor = Colors.Default) {
  let role = guild.roles.cache.find(r => r.name.toLowerCase() === roleName.toLowerCase());
  if (!role) {
    console.log(`Creating role '${roleName}' in guild ${guild.id}`);
    try {
      role = await guild.roles.create({
        name: roleName,
        color: roleColor,
        permissions: [],
        hoist: false,
        mentionable: false,
        reason: `Role for bot feature: ${roleName}`
      });
    } catch (error) {
      console.error(`Failed to create role '${roleName}' in guild ${guild.id}:`, error);
      throw new Error(`Could not create role '${roleName}'. Please check bot permissions (Manage Roles).`);
    }
  }
  return role;
}

async function setupStreakSystem(guild, category, config) {
  const channelName = 'streak-updates';
  const role2DayName = '2 Day Streak';
  const role7DayName = '7 Day Streak';

  const outputChannel = await getOrCreateProtectedChannel(guild, category, channelName);
  const role2Day = await getOrCreateRole(guild, role2DayName, Colors.Green);
  const role7Day = await getOrCreateRole(guild, role7DayName, Colors.Gold);

  config.streakSystem = {
    ...config.streakSystem,
    enabled: true,
    channelStreakOutput: outputChannel.id,
    role2day: role2Day.id,
    role7day: role7Day.id,
    streakThreshold: config.streakSystem?.streakThreshold ?? 4,
    isGymClassServer: config.streakSystem?.isGymClassServer ?? false,
  };
  console.log(`Streak System setup completed for guild ${guild.id}. Output: #${outputChannel.name}, Roles: '${role2Day.name}', '${role7Day.name}'`);
}

async function setupMessageLeaderSystem(guild, category, config) {
  const channelName = 'leaderboard-announcements';
  const roleName = 'Message Leader';

  const leaderChannel = await getOrCreateProtectedChannel(guild, category, channelName);
  const leaderRole = await getOrCreateRole(guild, roleName, Colors.Blue);

  config.messageLeaderSystem = {
    ...config.messageLeaderSystem,
    enabled: true,
    channelMessageLeader: leaderChannel.id,
    roleMessageLeader: leaderRole.id,
  };
  console.log(`Message Leader System setup completed for guild ${guild.id}. Channel: #${leaderChannel.name}, Role: '${leaderRole.name}'`);
}

async function setupLevelSystem(guild, category, config) {
  const channelName = 'level-ups';
  const roleLevel1Name = 'Level 1';
  const roleLevel5Name = 'Level 5';

  const levelUpChannel = await getOrCreateProtectedChannel(guild, category, channelName);
  const roleLevel1 = await getOrCreateRole(guild, roleLevel1Name, Colors.Aqua);
  const roleLevel5 = await getOrCreateRole(guild, roleLevel5Name, Colors.Purple);


  config.levelSystem = {
    ...config.levelSystem,
    enabled: true,
    channelLevelUp: levelUpChannel.id,
    roleLevel1: roleLevel1.id,
    roleLevel5: roleLevel5.id,
    xpPerMessage: config.levelSystem?.xpPerMessage ?? 10,
    levelMultiplier: config.levelSystem?.levelMultiplier ?? 1.5,
    levelUpMessages: config.levelSystem?.levelUpMessages ?? true,
  };
  console.log(`Level System setup completed for guild ${guild.id}. Channel: #${levelUpChannel.name}, Roles: '${roleLevel1.name}', '${roleLevel5.name}'`);
}


module.exports = {
  data: new SlashCommandBuilder()
      .setName('setup-bot')
      .setDescription('Sets up default channels & roles for selected bot systems.')
      .setDefaultMemberPermissions(PermissionsBitField.Flags.ManageGuild)
      .addStringOption(option =>
          option.setName('system')
              .setDescription('Select which system(s) to perform initial setup for')
              .setRequired(true)
              .addChoices(
                  { name: 'Streak System', value: 'streak-system' },
                  { name: 'Message Leader System', value: 'message-leader-system' },
                  { name: 'Level System', value: 'level-system' },
                  { name: 'All Engagement Systems', value: 'all-systems' }
              )
      ),

  async execute(interaction) {
    if (!interaction.memberPermissions?.has(PermissionsBitField.Flags.ManageGuild)) {
      return interaction.reply({
        content: 'You must have the "Manage Server" permission to use this command.',
        ephemeral: true
      });
    }
    if (!interaction.guild) {
      return interaction.reply({ content: 'This command can only be used in a server.', ephemeral: true });
    }

    try {
      await interaction.deferReply({ ephemeral: true });
      const guild = interaction.guild;
      const selectedSystem = interaction.options.getString('system');
      const configCategoryName = "Bot Systems";

      let config = await getConfig(guild.id) || {};
      ensureConfigStructure(config);

      const category = await getOrCreateCategory(guild, configCategoryName);
      if (!category) {
        return interaction.editReply({ content: `Failed to create the '${configCategoryName}' category. Please check bot permissions.` });
      }

      let setupPerformed = [];

      if (selectedSystem === 'streak-system' || selectedSystem === 'all-systems') {
        await setupStreakSystem(guild, category, config);
        setupPerformed.push("Streak System");
      }
      if (selectedSystem === 'message-leader-system' || selectedSystem === 'all-systems') {
        await setupMessageLeaderSystem(guild, category, config);
        setupPerformed.push("Message Leader System");
      }
      if (selectedSystem === 'level-system' || selectedSystem === 'all-systems') {
        await setupLevelSystem(guild, category, config);
        setupPerformed.push("Level System");
      }

      await saveConfig(guild.id, config);

      await interaction.editReply(`✅ Setup completed successfully! Configured: **${setupPerformed.join(', ')}**. Channels and roles created/verified under the '${configCategoryName}' category. You can further customize settings using \`/stitches-configuration\`.`);

    } catch (error) {
      console.error(`Error executing command setup-bot for guild ${interaction.guildId}:`, error);
      const errorMsg = `An error occurred during setup: ${error.message}. Please check bot permissions (Manage Channels, Manage Roles) and try again.`;
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({ content: errorMsg }).catch(console.error);
      } else {
        await interaction.reply({ content: errorMsg, ephemeral: true }).catch(console.error);
      }
    }
  }
};