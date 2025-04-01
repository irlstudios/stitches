const { SlashCommandBuilder } = require('@discordjs/builders');
const { PermissionsBitField } = require('discord.js');
const { getUserData, updateUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

module.exports = {
  data: new SlashCommandBuilder()
      .setName('edit-user-data')
      .setDescription('Edit a user\'s data (message count, streak, threshold, receivedDaily, XP, level, etc.).')
      .addUserOption(option =>
          option.setName('target')
              .setDescription('Select the user to edit')
              .setRequired(true))
      .addStringOption(option =>
          option.setName('field')
              .setDescription('Field to edit')
              .setRequired(true)
              .addChoices(
                  { name: 'Message Count', value: 'messages' },
                  { name: 'Streak Count', value: 'streak' },
                  { name: 'Threshold', value: 'threshold' },
                  { name: 'Received Daily', value: 'receivedDaily' },
                  { name: 'Level', value: 'level' },
                  { name: 'Active Days Count', value: 'activeDaysCount' }
              ))
      .addStringOption(option =>
          option.setName('value')
              .setDescription('New value for the field')
              .setRequired(true)),
  async execute(interaction) {
    if (!interaction.member.permissions.has(PermissionsBitField.Flags.ManageGuild)) {
      return interaction.reply({ content: 'You do not have permission to use this command.', ephemeral: true });
    }
    const targetUser = interaction.options.getUser('target');
    const field = interaction.options.getString('field');
    const value = interaction.options.getString('value');
    const guildId = interaction.guild.id;
    const client = interaction.client;
    const config = await getConfig(guildId);
    if (!config) {
      return interaction.reply({ content: 'Configuration not found for this guild.', ephemeral: true });
    }
    let userData = await getUserData(guildId, targetUser.id) || {
      messages: 0,
      streak: 0,
      highestStreak: 0,
      threshold: config.streakSystem?.streakThreshold || 10,
      receivedDaily: false,
      experience: { totalXp: 0, level: 0 },
      activeDaysCount: 0,
      longestInactivePeriod: 0,
      messageHeatmap: [],
      expireAt: null,
      lastUpdated: new Date().toISOString()
    };
    const updateFields = {};
    if (['messages', 'streak', 'level', 'activeDaysCount', 'longestInactivePeriod'].includes(field)) {
      if (isNaN(value) || parseInt(value, 10) < 0) {
        return interaction.reply({ content: 'Please enter a valid number for this field.', ephemeral: true });
      }
      if (field === 'level') {
        updateFields['experience.level'] = parseInt(value, 10);
      } else {
        updateFields[field] = parseInt(value, 10);
      }
    } else if (field === 'threshold') {
      if (isNaN(value) || parseInt(value, 10) < 0 || parseInt(value, 10) > (config.streakSystem?.streakThreshold || 10)) {
        return interaction.reply({ content: `Please enter a valid number for the threshold, not exceeding ${config.streakSystem?.streakThreshold || 10}.`, ephemeral: true });
      }
      updateFields[field] = parseInt(value, 10);
      updateFields.receivedDaily = false;
    } else if (field === 'receivedDaily') {
      if (!['true', 'false'].includes(value.toLowerCase())) {
        return interaction.reply({ content: 'Please enter either "true" or "false" for receivedDaily.', ephemeral: true });
      }
      updateFields.receivedDaily = value.toLowerCase() === 'true';
    } else {
      return interaction.reply({ content: 'Invalid field specified.', ephemeral: true });
    }
    if (field === 'streak') {
      const newStreak = parseInt(value, 10);
      if (newStreak > userData.highestStreak) {
        updateFields.highestStreak = newStreak;
      }
      for (let i = 1; i <= userData.streak; i++) {
        const roleKey = `role${i}day`;
        const roleId = config.streakSystem?.[roleKey];
        if (roleId) await removeRole(client, guildId, targetUser.id, roleId);
      }
      for (let i = 1; i <= newStreak; i++) {
        const roleKey = `role${i}day`;
        const roleId = config.streakSystem?.[roleKey];
        if (roleId) await assignRole(client, guildId, targetUser.id, roleId);
      }
    }
    if (field === 'level') {
      const newLevel = parseInt(value, 10);
      const oldLevel = userData.experience.level;
      const xpRequired = Math.floor(100 * Math.pow(config.levelSystem.levelMultiplier, newLevel));
      updateFields['experience.totalXp'] = Math.min(userData.experience.totalXp, xpRequired - 1);
      for (let i = 1; i <= oldLevel; i++) {
        const roleKey = `roleLevel${i}`;
        const roleId = config.levelSystem?.[roleKey];
        if (roleId) await removeRole(client, guildId, targetUser.id, roleId);
      }
      for (let i = 1; i <= newLevel; i++) {
        const roleKey = `roleLevel${i}`;
        const roleId = config.levelSystem?.[roleKey];
        if (roleId) await assignRole(client, guildId, targetUser.id, roleId);
      }
    }
    await updateUserData(targetUser.id, updateFields);
    await interaction.reply({ content: `Successfully updated ${field} for ${targetUser.username}.`, ephemeral: true });
  }
};

async function assignRole(client, guildId, userId, roleId) {
  try {
    const guild = client.guilds.cache.get(guildId);
    if (!guild) return;
    const member = await guild.members.fetch(userId);
    if (!member) return;
    const role = guild.roles.cache.get(roleId);
    if (role && !member.roles.cache.has(roleId)) {
      await member.roles.add(role);
    }
  } catch (error) {
    console.error(`Error assigning role ${roleId} to user ${userId}: ${error.message}`);
  }
}

async function removeRole(client, guildId, userId, roleId) {
  try {
    const guild = client.guilds.cache.get(guildId);
    if (!guild) return;
    const member = await guild.members.fetch(userId);
    if (!member) return;
    const role = guild.roles.cache.get(roleId);
    if (role && member.roles.cache.has(roleId)) {
      await member.roles.remove(role);
    }
  } catch (error) {
    console.error(`Error removing role ${roleId} from user ${userId}: ${error.message}`);
  }
}