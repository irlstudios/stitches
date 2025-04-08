const { SlashCommandBuilder } = require('@discordjs/builders');
const { PermissionsBitField } = require('discord.js');
const { getUserData, updateUserData } = require('../dynamoDB');
const { getConfig } = require('../configManager');

async function syncStreakRoles(client, guildId, userId, config, oldStreak, newStreak) {
  if (!config?.streakSystem?.enabled) return;

  const guild = client.guilds.cache.get(guildId);
  if (!guild) { console.warn(`[syncStreakRoles] Guild ${guildId} not found.`); return; }
  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) { console.warn(`[syncStreakRoles] Member ${userId} not found in guild ${guildId}.`); return; }

  const rolesToAdd = [];
  const rolesToRemove = [];

  for (const key in config.streakSystem) {
    if (key.startsWith('role') && key.endsWith('day')) {
      const threshold = parseInt(key.replace('role', '').replace('day', ''), 10);
      const roleId = config.streakSystem[key];
      if (isNaN(threshold) || !roleId) continue;

      const hadRoleBasedOnOld = oldStreak >= threshold;
      const shouldHaveRoleBasedOnNew = newStreak >= threshold;
      const currentlyHasRole = member.roles.cache.has(roleId);

      if (shouldHaveRoleBasedOnNew && !currentlyHasRole) {
        rolesToAdd.push(roleId);
      } else if (!shouldHaveRoleBasedOnNew && currentlyHasRole) {
        rolesToRemove.push(roleId);
      }
    }
  }

  try {
    if (rolesToRemove.length > 0) await member.roles.remove(rolesToRemove, `Manual data edit: Streak changed from ${oldStreak} to ${newStreak}`);
    if (rolesToAdd.length > 0) await member.roles.add(rolesToAdd, `Manual data edit: Streak changed to ${newStreak}`);
    console.log(`Synced streak roles for ${userId} (Old: ${oldStreak}, New: ${newStreak}). Added: [${rolesToAdd.join()}], Removed: [${rolesToRemove.join()}]`);
  } catch (error) {
    console.error(`Error syncing streak roles for ${userId} in ${guildId} after manual edit:`, error);
    if (error.code === 50013) {
      console.error(`[syncStreakRoles] Missing Permissions in guild ${guildId}.`);
    }
  }
}

async function syncLevelRoles(client, guildId, userId, config, oldLevel, newLevel) {
  if (!config?.levelSystem?.enabled) return;

  const guild = client.guilds.cache.get(guildId);
  if (!guild) { console.warn(`[syncLevelRoles] Guild ${guildId} not found.`); return; }
  const member = await guild.members.fetch(userId).catch(() => null);
  if (!member) { console.warn(`[syncLevelRoles] Member ${userId} not found in guild ${guildId}.`); return; }

  const rolesToAdd = [];
  const rolesToRemove = [];

  for (const key in config.levelSystem) {
    if (key.startsWith('roleLevel')) {
      const threshold = parseInt(key.replace('roleLevel', ''), 10);
      const roleId = config.levelSystem[key];
      if (isNaN(threshold) || !roleId) continue;

      const hadRoleBasedOnOld = oldLevel >= threshold;
      const shouldHaveRoleBasedOnNew = newLevel >= threshold;
      const currentlyHasRole = member.roles.cache.has(roleId);

      if (shouldHaveRoleBasedOnNew && !currentlyHasRole) {
        rolesToAdd.push(roleId);
      } else if (!shouldHaveRoleBasedOnNew && currentlyHasRole) {
        rolesToRemove.push(roleId);
      }
    }
  }

  try {
    if (rolesToRemove.length > 0) await member.roles.remove(rolesToRemove, `Manual data edit: Level changed from ${oldLevel} to ${newLevel}`);
    if (rolesToAdd.length > 0) await member.roles.add(rolesToAdd, `Manual data edit: Level changed to ${newLevel}`);
    console.log(`Synced level roles for ${userId} (Old: ${oldLevel}, New: ${newLevel}). Added: [${rolesToAdd.join()}], Removed: [${rolesToRemove.join()}]`);
  } catch (error) {
    console.error(`Error syncing level roles for ${userId} in ${guildId} after manual edit:`, error);
    if (error.code === 50013) {
      console.error(`[syncLevelRoles] Missing Permissions in guild ${guildId}.`);
    }
  }
}

module.exports = {
  data: new SlashCommandBuilder()
      .setName('edit-user-data')
      .setDescription('MANUAL EDIT: Modify a user\'s stored data. Use with extreme caution!')
      .setDefaultMemberPermissions(PermissionsBitField.Flags.ManageGuild)
      .addUserOption(option =>
          option.setName('target')
              .setDescription('Select the user whose data you want to edit')
              .setRequired(true))
      .addStringOption(option =>
          option.setName('field')
              .setDescription('The specific data field to edit (Use dot notation for nested, e.g., experience.level)')
              .setRequired(true)
              .addChoices(
                  { name: 'Current Streak', value: 'streak' },
                  { name: 'Highest Streak', value: 'highestStreak' },
                  { name: 'Messages (Weekly Count)', value: 'messages' },
                  { name: 'Total Messages (Lifetime)', value: 'totalMessages' },
                  { name: 'Streak Threshold', value: 'threshold' },
                  { name: 'Received Daily Flag', value: 'receivedDaily' },
                  { name: 'Level', value: 'experience.level' },
                  { name: 'Total XP', value: 'experience.totalXp' },
                  { name: 'Message Leader Wins', value: 'messageLeaderWins' },
                  { name: 'Most Consecutive Leader Wins', value: 'mostConsecutiveLeader' },
                  { name: 'Active Days Count', value: 'activeDaysCount' },
                  { name: 'Days Tracked', value: 'daysTracked' },
                  { name: 'Longest Inactive Period', value: 'longestInactivePeriod' },
                  { name: 'XP Booster Multiplier', value: 'boosters' }
              ))
      .addStringOption(option =>
          option.setName('value')
              .setDescription('The new value for the selected field (use true/false for flags)')
              .setRequired(true)),

  async execute(interaction) {
    if (!interaction.memberPermissions?.has(PermissionsBitField.Flags.ManageGuild)) {
      return interaction.reply({ content: 'You must have the "Manage Server" permission to use this command.', ephemeral: true });
    }

    const targetUser = interaction.options.getUser('target');
    const fieldToEdit = interaction.options.getString('field');
    const newValueString = interaction.options.getString('value');
    const { guild, client } = interaction;
    const guildId = guild.id;


    try {
      await interaction.deferReply({ ephemeral: true });

      const config = await getConfig(guildId);
      if (!config) {
        return interaction.editReply({ content: 'Bot configuration is missing for this server.' });
      }

      let userData = await getUserData(guildId, targetUser.id);

      if (!userData) {
        return interaction.editReply({ content: `No data found for ${targetUser.username}. Cannot edit.` });
      }

      const updates = {};
      let parsedValue;
      const numericFields = [
        'streak', 'highestStreak', 'messages', 'totalMessages', 'threshold',
        'messageLeaderWins', 'mostConsecutiveLeader', 'activeDaysCount',
        'daysTracked', 'longestInactivePeriod', 'boosters',
        'experience.level', 'experience.totalXp'
      ];
      const booleanFields = ['receivedDaily'];

      if (numericFields.includes(fieldToEdit)) {
        if (fieldToEdit === 'boosters' || fieldToEdit.includes('average')) {
          parsedValue = parseFloat(newValueString);
        } else {
          parsedValue = parseInt(newValueString, 10);
        }

        if (isNaN(parsedValue) || parsedValue < 0) {
          return interaction.editReply({ content: `Please enter a valid non-negative number for ${fieldToEdit}.`, ephemeral: true });
        }

        if (fieldToEdit === 'threshold') {
          const maxThreshold = config.streakSystem?.streakThreshold || 10;
          if (parsedValue > maxThreshold) {
            return interaction.editReply({ content: `Threshold cannot exceed the configured maximum of ${maxThreshold}.`, ephemeral: true });
          }
          updates.receivedDaily = false;
        }
        updates[fieldToEdit] = parsedValue;

      } else if (booleanFields.includes(fieldToEdit)) {
        const lowerVal = newValueString.toLowerCase();
        if (!['true', 'false', '1', '0'].includes(lowerVal)) {
          return interaction.editReply({ content: 'Please enter true/false or 1/0 for this flag.', ephemeral: true });
        }
        parsedValue = (lowerVal === 'true' || lowerVal === '1');
        updates[fieldToEdit] = parsedValue;

      } else {
        return interaction.editReply({ content: 'Invalid field specified.', ephemeral: true });
      }


      let oldStreak = userData.streak || 0;
      let oldLevel = userData.experience?.level || 0;

      if (fieldToEdit === 'streak') {
        const newStreak = updates.streak;
        if (newStreak > (userData.highestStreak || 0)) {
          updates.highestStreak = newStreak;
        }
        await syncStreakRoles(client, guildId, targetUser.id, config, oldStreak, newStreak);
      }

      if (fieldToEdit === 'experience.level') {
        const newLevel = updates['experience.level'];
         if (newLevel < oldLevel) {
             const xpForPrevLevel = newLevel > 0 ? Math.floor(100 * Math.pow(config.levelSystem.levelMultiplier || 1.5, newLevel - 1)) : 0;
             const currentXp = userData.experience?.totalXp || 0;
             updates['experience.totalXp'] = Math.min(currentXp, xpForPrevLevel -1);
         }
        await syncLevelRoles(client, guildId, targetUser.id, config, oldLevel, newLevel);
      }


      await updateUserData(guildId, targetUser.id, updates);

      await interaction.editReply({ content: `✅ Successfully updated field \`${fieldToEdit}\` to \`${newValueString}\` for ${targetUser.username}.` });

    } catch (error) {
      console.error(`Error editing user data for ${targetUser.id} in guild ${guildId}:`, error);
      const errorMsg = 'An error occurred while trying to edit user data.';
      if (interaction.deferred || interaction.replied) {
        await interaction.editReply({ content: errorMsg }).catch(console.error);
      } else {
        await interaction.reply({ content: errorMsg, ephemeral: true }).catch(console.error);
      }
    }
  }
};